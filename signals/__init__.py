"""
Signal Handling

Provides advanced signal handling for async applications:
- Signal registration with coroutines
- Graceful shutdown coordination
- Signal propagation to subprocesses
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import logging
import signal
import sys
from collections.abc import Callable, Coroutine
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

from ..core.exceptions import SignalError
from ..core.loop import DebugEventLoop, get_event_loop
from ..tasks import TaskScope

logger = logging.getLogger(__name__)

_shutdown_initiated: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "aevum_shutdown_initiated", default=False
)
_shutdown_handlers: contextvars.ContextVar[list[Callable]] = contextvars.ContextVar(
    "aevum_shutdown_handlers", default=[]
)


@dataclass(slots=True)
class SignalInfo:
    """Information about a received signal."""

    signum: int
    name: str
    timestamp: float
    frame: Any | None = None


class SignalHandler:
    """
    Manages signal handlers for the event loop.

    Supports:
    - Multiple handlers per signal
    - Async handlers
    - Priority ordering
    - One-time handlers
    """

    def __init__(self, loop: DebugEventLoop | None = None):
        self._loop = loop or get_event_loop()
        self._handlers: dict[int, list[tuple[Callable, bool, int]]] = {}  # (handler, once, priority)
        self._received_signals: list[SignalInfo] = []
        self._max_history = 100

    def add_handler(
        self,
        sig: signal.Signals | int,
        handler: Callable[..., Any] | Coroutine[Any, Any, Any],
        *,
        once: bool = False,
        priority: int = 0,
    ) -> None:
        """Add a signal handler."""
        signum = int(sig)
        handlers = self._handlers.setdefault(signum, [])

        # Check if already registered
        for h, _, _ in handlers:
            if h is handler:
                return

        handlers.append((handler, once, priority))
        handlers.sort(key=lambda x: -x[2])  # Higher priority first

        # Register with loop
        try:
            self._loop.add_signal_handler(signum, self._dispatch_signal, signum)
        except (ValueError, OSError) as e:
            logger.warning(f"Could not register signal {signum}: {e}")

    def remove_handler(self, sig: signal.Signals | int, handler: Callable) -> bool:
        """Remove a signal handler."""
        signum = int(sig)
        handlers = self._handlers.get(signum, [])

        for i, (h, _, _) in enumerate(handlers):
            if h is handler:
                handlers.pop(i)
                if not handlers:
                    try:
                        self._loop.remove_signal_handler(signum)
                    except (ValueError, OSError):
                        pass
                return True
        return False

    def _dispatch_signal(self, signum: int) -> None:
        """Dispatch signal to handlers."""
        info = SignalInfo(
            signum=signum,
            name=signal.Signals(signum).name if signum in signal.Signals.__members__.values() else str(signum),
            timestamp=asyncio.get_event_loop().time(),
        )
        self._received_signals.append(info)
        if len(self._received_signals) > self._max_history:
            self._received_signals.pop(0)

        handlers = self._handlers.get(signum, [])
        for handler, once, _ in handlers[:]:
            try:
                result = handler(signum, info.frame)
                if asyncio.iscoroutine(result):
                    self._loop.create_task(result, name=f"signal-handler-{info.name}")
            except Exception as e:
                logger.exception(f"Signal handler error for {info.name}: {e}")

            if once:
                self.remove_handler(signum, handler)

    def get_received_signals(self) -> list[SignalInfo]:
        """Get history of received signals."""
        return self._received_signals.copy()

    def clear_history(self) -> None:
        """Clear signal history."""
        self._received_signals.clear()


# Default signal handler
_default_signal_handler: SignalHandler | None = None


def get_signal_handler(loop: DebugEventLoop | None = None) -> SignalHandler:
    """Get the default signal handler."""
    global _default_signal_handler
    if _default_signal_handler is None:
        _default_signal_handler = SignalHandler(loop)
    return _default_signal_handler


def handle_signal(
    sig: signal.Signals | int,
    handler: Callable[..., Any] | Coroutine[Any, Any, Any],
    *,
    once: bool = False,
    priority: int = 0,
    loop: DebugEventLoop | None = None,
) -> None:
    """Register a signal handler."""
    get_signal_handler(loop).add_handler(sig, handler, once=once, priority=priority)


def ignore_signal(sig: signal.Signals | int, loop: DebugEventLoop | None = None) -> None:
    """Ignore a signal."""
    signum = int(sig)
    loop = loop or get_event_loop()
    try:
        loop.add_signal_handler(signum, lambda: None)
    except (ValueError, OSError):
        pass


def default_signal_handler(sig: signal.Signals | int, loop: DebugEventLoop | None = None) -> None:
    """Restore default signal handler."""
    signum = int(sig)
    loop = loop or get_event_loop()
    try:
        loop.remove_signal_handler(signum)
    except (ValueError, OSError):
        pass
    signal.signal(signum, signal.SIG_DFL)


class GracefulShutdown:
    """
    Coordinates graceful shutdown on signals.

    Features:
    - Multiple shutdown stages
    - Timeout per stage
    - Signal propagation
    - Health check integration
    """

    def __init__(
        self,
        *,
        loop: DebugEventLoop | None = None,
        signals: list[signal.Signals | int] | None = None,
        timeout: float = 30.0,
        stages: list[tuple[str, float]] | None = None,
    ):
        self._loop = loop or get_event_loop()
        self._signals = signals or [signal.SIGTERM, signal.SIGINT]
        self._timeout = timeout
        self._stages = stages or [
            ("prepare", 5.0),
            ("drain", 10.0),
            ("shutdown", 10.0),
            ("force", 5.0),
        ]
        self._stage_handlers: dict[str, list[Callable]] = {s[0]: [] for s in self._stages}
        self._shutdown_event = asyncio.Event()
        self._shutdown_complete = asyncio.Event()
        self._initiated = False

    def on_stage(self, stage: str) -> Callable:
        """Decorator to register a handler for a shutdown stage."""
        def decorator(func: Callable) -> Callable:
            self._stage_handlers.setdefault(stage, []).append(func)
            return func
        return decorator

    def add_handler(self, stage: str, handler: Callable) -> None:
        """Add a handler for a shutdown stage."""
        self._stage_handlers.setdefault(stage, []).append(handler)

    async def initiate(self, signum: int | None = None) -> None:
        """Initiate graceful shutdown."""
        if self._initiated:
            return
        self._initiated = True
        _shutdown_initiated.set(True)
        logger.info(f"Graceful shutdown initiated by signal {signum}")

        # Run handlers in stages
        for stage_name, stage_timeout in self._stages:
            logger.info(f"Shutdown stage: {stage_name} (timeout: {stage_timeout}s)")
            handlers = self._stage_handlers.get(stage_name, [])

            if handlers:
                async with TaskScope(loop=self._loop, timeout=stage_timeout) as scope:
                    for handler in handlers:
                        scope.create_task(handler(), name=f"shutdown-{stage_name}-{handler.__name__}")
                    await scope.wait_all()

        self._shutdown_event.set()
        self._shutdown_complete.set()
        logger.info("Graceful shutdown complete")

    async def wait_for_shutdown(self) -> None:
        """Wait for shutdown to be initiated."""
        await self._shutdown_event.wait()

    async def wait_for_complete(self) -> None:
        """Wait for shutdown to complete."""
        await self._shutdown_complete.wait()

    @property
    def is_initiated(self) -> bool:
        return self._initiated


def on_shutdown(handler: Callable) -> Callable:
    """Decorator to register a shutdown handler (runs in 'shutdown' stage)."""
    shutdown = get_graceful_shutdown()
    shutdown.add_handler("shutdown", handler)
    return handler


_graceful_shutdown: GracefulShutdown | None = None


def get_graceful_shutdown(loop: DebugEventLoop | None = None) -> GracefulShutdown:
    """Get the default graceful shutdown coordinator."""
    global _graceful_shutdown
    if _graceful_shutdown is None:
        _graceful_shutdown = GracefulShutdown(loop=loop)
        # Register default signal handlers
        for sig in _graceful_shutdown._signals:
            handle_signal(sig, _graceful_shutdown.initiate, loop=loop)
    return _graceful_shutdown


async def wait_for_shutdown() -> None:
    """Wait for graceful shutdown to be initiated."""
    await get_graceful_shutdown().wait_for_shutdown()


@asynccontextmanager
async def shutdown_context(
    *,
    loop: DebugEventLoop | None = None,
    signals: list[signal.Signals | int] | None = None,
    timeout: float = 30.0,
) -> AsyncIterator[GracefulShutdown]:
    """Context manager for graceful shutdown."""
    shutdown = GracefulShutdown(loop=loop, signals=signals, timeout=timeout)
    try:
        yield shutdown
    finally:
        if not shutdown.is_initiated:
            await shutdown.initiate()


# Signal utilities
async def send_signal_to_process(pid: int, sig: signal.Signals | int) -> None:
    """Send signal to a process."""
    try:
        os.kill(pid, int(sig))
    except ProcessLookupError:
        raise SignalError(f"Process {pid} not found", signum=int(sig))
    except PermissionError:
        raise SignalError(f"Permission denied sending signal to {pid}", signum=int(sig))


def setup_signal_handlers(
    loop: DebugEventLoop | None = None,
    *,
    sigterm: Callable | None = None,
    sigint: Callable | None = None,
    sigusr1: Callable | None = None,
    sigusr2: Callable | None = None,
) -> None:
    """Quick setup for common signal handlers."""
    loop = loop or get_event_loop()

    if sigterm:
        handle_signal(signal.SIGTERM, sigterm, loop=loop)
    if sigint:
        handle_signal(signal.SIGINT, sigint, loop=loop)
    if sigusr1:
        handle_signal(signal.SIGUSR1, sigusr1, loop=loop)
    if sigusr2:
        handle_signal(signal.SIGUSR2, sigusr2, loop=loop)

    # Ignore SIGPIPE on Unix
    if sys.platform != "win32" and hasattr(signal, "SIGPIPE"):
        ignore_signal(signal.SIGPIPE, loop=loop)


# Windows-specific signal handling
if sys.platform == "win32":
    def install_signal_handler(sig: int, handler: Callable) -> None:
        """Install signal handler on Windows."""
        signal.signal(sig, handler)

    def remove_signal_handler(sig: int) -> None:
        """Remove signal handler on Windows."""
        signal.signal(sig, signal.SIG_DFL)
else:
    def install_signal_handler(sig: int, handler: Callable) -> None:
        """Install signal handler on Unix."""
        loop = get_event_loop()
        loop.add_signal_handler(sig, handler)

    def remove_signal_handler(sig: int) -> None:
        """Remove signal handler on Unix."""
        loop = get_event_loop()
        try:
            loop.remove_signal_handler(sig)
        except (ValueError, OSError):
            pass


@asynccontextmanager
async def temporary_signal_handler(
    sig: signal.Signals | int,
    handler: Callable,
    loop: DebugEventLoop | None = None,
):
    """Temporarily install a signal handler."""
    loop = loop or get_event_loop()
    signum = int(sig)

    # Save old handler
    old_handler = None
    try:
        old_handler = signal.signal(signum, signal.SIG_IGN)
    except (OSError, ValueError):
        pass

    # Install new handler
    handle_signal(sig, handler, loop=loop)

    try:
        yield
    finally:
        # Remove our handler
        get_signal_handler(loop).remove_handler(sig, handler)
        # Restore old handler
        if old_handler:
            try:
                signal.signal(signum, old_handler)
            except (OSError, ValueError):
                pass