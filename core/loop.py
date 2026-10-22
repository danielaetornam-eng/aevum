"""
Enhanced Event Loop Implementation

The DebugEventLoop extends asyncio's event loop with:
- Comprehensive debugging and profiling
- Built-in metrics collection
- Task tracing and slow callback detection
- Structured concurrency support
- Extension points for framework builders
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import logging
import os
import signal
import sys
import threading
import time
import traceback
import warnings
from collections import deque
from collections.abc import Awaitable, Callable, Coroutine, Generator, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, ClassVar, TypeVar, cast

from .config import LoopConfig, LoopMetrics, LoopState
from .exceptions import (
    AevumError,
    LoopClosedError,
    LoopConfigurationError,
    TaskError,
    TaskTimeoutError,
)
from .context import LoopContext
from ._context_vars import _loop_context_var, _handle_signals_var, current_loop, set_current_loop

if sys.version_info >= (3, 11):
    from asyncio import TaskGroup as _TaskGroup
else:
    from asyncio import TaskGroup as _TaskGroup

T = TypeVar("T")

logger = logging.getLogger(__name__)

_handle_signals_var: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "aevum_handle_signals", default=True
)


@dataclass(slots=True)
class _CallbackTrace:
    """Trace information for a callback execution."""

    name: str
    start_time: float
    duration: float = 0.0
    is_slow: bool = False
    stack: list[str] = field(default_factory=list)
    task: asyncio.Task | None = None


@dataclass(slots=True)
class _TaskTrace:
    """Trace information for a task."""

    name: str
    coro: Coroutine[Any, Any, Any]
    created_at: float
    created_stack: list[str]
    started_at: float | None = None
    completed_at: float | None = None
    duration: float = 0.0
    is_slow: bool = False
    exception: BaseException | None = None
    cancelled: bool = False


# Platform-specific base class
if sys.platform == "win32":
    from asyncio.windows_events import ProactorEventLoop as _BaseEventLoop
else:
    try:
        from asyncio.unix_events import SelectorEventLoop as _BaseEventLoop
    except ImportError:
        from asyncio.selector_events import SelectorEventLoop as _BaseEventLoop


class DebugEventLoop(_BaseEventLoop):
    """
    Enhanced event loop with debugging, metrics, and tracing capabilities.

    This loop extends asyncio's base event loop with:
    - Automatic slow callback detection
    - Task lifecycle tracing
    - Built-in metrics collection
    - Configurable exception handling
    - Extension hooks for framework builders
    """

    _current_loop: ClassVar[DebugEventLoop | None] = None
    _loop_counter: ClassVar[int] = 0

    def __init__(self, config: LoopConfig | None = None):
        # Initialize state before super().__init__() because parent calls set_debug()
        DebugEventLoop._loop_counter += 1
        self._loop_id = DebugEventLoop._loop_counter
        self._config = config or LoopConfig()
        self._state = LoopState.CREATED
        self._metrics = LoopMetrics()
        self._metrics.loop_start_time = time.monotonic()

        super().__init__()

        # Debugging
        self._slow_callback_threshold = self._config.slow_callback_duration
        self._slow_task_threshold = self._config.slow_task_duration
        self._callback_traces: deque[_CallbackTrace] = deque(maxlen=1000)
        self._task_traces: dict[int, _TaskTrace] = {}

        # Task management
        self._task_factory = self._config.task_factory or asyncio.Task
        self._pending_tasks: set[asyncio.Task] = set()
        self._task_counter: int = 0

        # Exception handling
        self._exception_handler = self._config.exception_handler
        self._unhandled_exceptions: list[tuple[BaseException, dict]] = []

        # Extensions
        self._extensions: dict[str, Any] = {}
        self._extension_hooks: dict[str, list[Callable]] = {}

        # Thread safety
        self._thread_id = threading.get_ident()
        self._closing = False

        # Signal handling
        self._signal_handlers: dict[int, Callable] = {}
        self._original_signal_handlers: dict[int, Callable] = {}

        # Timer tracking
        self._timer_handles: dict[int, asyncio.TimerHandle] = {}
        self._timer_counter: int = 0

        # Context
        self._context = LoopContext(self)

        if self._config.debug:
            self.set_debug(True)

        DebugEventLoop._current_loop = self
        logger.info(f"Created DebugEventLoop #{self._loop_id}")

    @property
    def loop_id(self) -> int:
        return self._loop_id

    @property
    def config(self) -> LoopConfig:
        return self._config

    @property
    def metrics(self) -> LoopMetrics:
        return self._metrics

    @property
    def state(self) -> str:
        return self._state

    def is_running(self) -> bool:
        return self._state == LoopState.RUNNING

    def is_closed(self) -> bool:
        return self._state == LoopState.CLOSED

    def _check_closed(self) -> None:
        if self._state == LoopState.CLOSED:
            raise LoopClosedError(loop_id=self._loop_id)

    def _check_thread(self) -> None:
        current_thread = threading.get_ident()
        # Allow None thread_id during cleanup phase
        if self._thread_id is not None and current_thread != self._thread_id:
            raise RuntimeError(
                f"Event loop {self._loop_id} bound to different thread "
                f"({self._thread_id} vs {current_thread})"
            )

    # ========================================================================
    # Loop Control
    # ========================================================================

    def _run_forever_setup(self) -> None:
        """Prepare the run loop to process events."""
        super()._run_forever_setup()
        # Set state to RUNNING after base class setup to avoid _check_running conflict
        if self._state == LoopState.CREATED:
            self._state = LoopState.RUNNING

    def _run_forever_cleanup(self) -> None:
        """Clean up after an event loop finishes the looping over events."""
        super()._run_forever_cleanup()
        if self._state == LoopState.RUNNING:
            self._state = LoopState.STOPPED

    def run_forever(self) -> None:
        """Run the event loop until stop() is called."""
        self._check_closed()
        self._check_thread()

        if self._state == LoopState.RUNNING:
            raise RuntimeError("Event loop is already running")

        logger.info(f"Loop #{self._loop_id} started")

        try:
            super().run_forever()
        finally:
            logger.info(f"Loop #{self._loop_id} stopped")

    def run_until_complete(self, future: Awaitable[T]) -> T:
        """Run until the future is complete."""
        self._check_closed()
        self._check_thread()

        if self._state == LoopState.RUNNING:
            raise RuntimeError("Cannot run_until_complete on running loop")

        logger.info(f"Loop #{self._loop_id} running until complete")

        try:
            return super().run_until_complete(future)
        finally:
            logger.info(f"Loop #{self._loop_id} completed")

    def stop(self) -> None:
        """Stop the event loop."""
        if self._state == LoopState.CLOSED:
            return

        self._state = LoopState.STOPPING
        logger.info(f"Loop #{self._loop_id} stopping")
        super().stop()
        # After stop, the loop is no longer running
        if self._state == LoopState.STOPPING:
            self._state = LoopState.STOPPED

    def close(self) -> None:
        """Close the event loop."""
        if self._state == LoopState.CLOSED:
            return

        logger.info(f"Closing loop #{self._loop_id}")

        # Cancel all pending tasks
        for task in list(self._pending_tasks):
            if not task.done():
                task.cancel()

        # Run cleanup
        self._run_cleanup()

        # Close extensions
        for ext in self._extensions.values():
            if hasattr(ext, "close"):
                try:
                    ext.close()
                except Exception as e:
                    logger.exception(f"Error closing extension: {e}")

        # Restore signal handlers
        self._restore_signal_handlers()

        self._state = LoopState.CLOSED
        super().close()

        if DebugEventLoop._current_loop is self:
            DebugEventLoop._current_loop = None

        logger.info(f"Loop #{self._loop_id} closed")

    def _run_cleanup(self) -> None:
        """Run cleanup callbacks."""
        # Execute shutdown callbacks
        for hook in self._extension_hooks.get("shutdown", []):
            try:
                hook(self)
            except Exception as e:
                logger.exception(f"Error in shutdown hook: {e}")

    # ========================================================================
    # Task Management
    # ========================================================================

    def create_task(
        self,
        coro: Coroutine[Any, Any, T],
        *,
        name: str | None = None,
        timeout: float | None = None,
    ) -> asyncio.Task[T]:
        """Create a task with enhanced tracking."""
        self._check_closed()
        self._check_thread()

        if self._config.max_concurrent_tasks:
            active = sum(1 for t in self._pending_tasks if not t.done())
            if active >= self._config.max_concurrent_tasks:
                raise TaskError(
                    f"Max concurrent tasks ({self._config.max_concurrent_tasks}) reached",
                    task_name=name,
                )

        self._task_counter += 1
        task_name = name or f"Task-{self._task_counter}"

        if self._config.trace_task_creation:
            stack = traceback.format_stack()[:-1]
            trace = _TaskTrace(
                name=task_name,
                coro=coro,
                created_at=time.monotonic(),
                created_stack=stack,
            )
            self._task_traces[id(coro)] = trace

        task = self._task_factory(coro, loop=self, name=task_name)
        self._pending_tasks.add(task)

        self._metrics.tasks_created += 1

        def _task_done(t: asyncio.Task) -> None:
            self._pending_tasks.discard(t)
            self._metrics.tasks_pending = len([x for x in self._pending_tasks if not x.done()])

            if t.cancelled():
                self._metrics.tasks_cancelled += 1
            elif t.exception():
                self._metrics.tasks_failed += 1
            else:
                self._metrics.tasks_completed += 1

            if self._config.trace_task_creation:
                trace = self._task_traces.pop(id(t.get_coro()), None)
                if trace:
                    trace.completed_at = time.monotonic()
                    if trace.started_at:
                        trace.duration = trace.completed_at - trace.started_at
                    trace.cancelled = t.cancelled()
                    trace.exception = t.exception()
                    trace.is_slow = trace.duration > self._slow_task_threshold

        task.add_done_callback(_task_done)
        return task

    def create_task_group(self) -> _TaskGroup:
        """Create a structured concurrency task group."""
        return _TaskGroup(loop=self)

    async def wait_for(
        self,
        aw: Awaitable[T],
        timeout: float | None = None,
    ) -> T:
        """Wait for an awaitable with timeout."""
        if timeout is None:
            timeout = self._config.default_task_timeout
        if timeout is None:
            return await aw

        try:
            return await asyncio.wait_for(aw, timeout=timeout, loop=self)
        except asyncio.TimeoutError as e:
            raise TaskTimeoutError(timeout=timeout) from e

    def get_pending_tasks(self) -> set[asyncio.Task]:
        """Get all pending tasks."""
        return self._pending_tasks.copy()

    def get_task_traces(self) -> list[_TaskTrace]:
        """Get all task traces."""
        return list(self._task_traces.values())

    # ========================================================================
    # Callback Execution with Tracing
    # ========================================================================

    def _run_once(self) -> None:
        """Single iteration of the event loop with tracing."""
        self._metrics.loop_iterations += 1
        iteration_start = time.monotonic()

        # Count callbacks in ready queue before processing
        ready_count = len(self._ready)

        # Process timers and callbacks
        super()._run_once()

        # Update metrics
        self._metrics.callbacks_executed += ready_count
        self._metrics.callbacks_scheduled += ready_count

        iteration_duration = time.monotonic() - iteration_start
        self._metrics.last_iteration_time = iteration_duration
        self._metrics.total_callback_time += iteration_duration

        # Collect memory metrics periodically
        if self._metrics.loop_iterations % 100 == 0:
            self._update_memory_metrics()

    def _process_events(self, event_list: list[tuple[Any, Any]]) -> None:
        """Process I/O events with tracing."""
        for fd, events in event_list:
            if events & (asyncio.constants.READ | asyncio.constants.WRITE):
                self._metrics.io_reads += 1

    def call_soon(
        self,
        callback: Callable[..., Any],
        *args: Any,
        context: contextvars.Context | None = None,
    ) -> asyncio.Handle:
        """Schedule a callback with tracing."""
        self._check_closed()

        if self._config.enable_tracing:
            traced_callback = self._trace_callback(callback)
            return super().call_soon(traced_callback, *args, context=context)

        return super().call_soon(callback, *args, context=context)

    def call_later(
        self,
        delay: float,
        callback: Callable[..., Any],
        *args: Any,
        context: contextvars.Context | None = None,
    ) -> asyncio.TimerHandle:
        """Schedule a callback after delay with tracing."""
        self._check_closed()

        if self._config.enable_tracing:
            traced_callback = self._trace_callback(callback)
            return super().call_later(delay, traced_callback, *args, context=context)

        return super().call_later(delay, callback, *args, context=context)

    def call_at(
        self,
        when: float,
        callback: Callable[..., Any],
        *args: Any,
        context: contextvars.Context | None = None,
    ) -> asyncio.TimerHandle:
        """Schedule a callback at absolute time with tracing."""
        self._check_closed()

        if self._config.enable_tracing:
            traced_callback = self._trace_callback(callback)
            return super().call_at(when, traced_callback, *args, context=context)

        return super().call_at(when, callback, *args, context=context)

    def _trace_callback(self, callback: Callable) -> Callable:
        """Wrap callback with tracing."""

        @functools.wraps(callback)
        def traced(*args: Any, **kwargs: Any) -> Any:
            name = getattr(callback, "__name__", str(callback))
            start = time.monotonic()

            trace = _CallbackTrace(
                name=name,
                start_time=start,
                stack=traceback.format_stack()[:-1] if self._config.debug else [],
            )

            try:
                result = callback(*args, **kwargs)
                return result
            except Exception as e:
                self._handle_callback_exception(e, callback)
                raise
            finally:
                duration = time.monotonic() - start
                trace.duration = duration
                trace.is_slow = duration > self._slow_callback_threshold

                self._callback_traces.append(trace)
                self._metrics.callbacks_executed += 1
                self._metrics.total_callback_time += duration

                if trace.is_slow:
                    self._metrics.slow_callbacks += 1
                    if duration > self._metrics.slowest_callback_time:
                        self._metrics.slowest_callback_time = duration
                        self._metrics.slowest_callback_name = name

                    if self._config.trace_slow_callbacks:
                        logger.warning(
                            f"Slow callback detected: {name} took {duration:.3f}s "
                            f"(threshold: {self._slow_callback_threshold}s)"
                        )

        return traced

    def _handle_callback_exception(self, exc: BaseException, callback: Callable) -> None:
        """Handle exception from callback."""
        self._metrics.exceptions_handled += 1

        context = {
            "callback": getattr(callback, "__name__", str(callback)),
            "loop_id": self._loop_id,
            "timestamp": time.time(),
        }

        if self._exception_handler:
            try:
                self._exception_handler(self, {"exception": exc, **context})
            except Exception as e:
                logger.exception(f"Exception handler failed: {e}")
                self._unhandled_exceptions.append((exc, context))
                self._metrics.exceptions_unhandled += 1
        else:
            self.default_exception_handler({"exception": exc, **context})
            self._unhandled_exceptions.append((exc, context))
            self._metrics.exceptions_unhandled += 1

    # ========================================================================
    # Timer Management
    # ========================================================================

    def call_later(
        self,
        delay: float,
        callback: Callable[..., Any],
        *args: Any,
        context: contextvars.Context | None = None,
    ) -> asyncio.TimerHandle:
        self._timer_counter += 1
        timer_id = self._timer_counter
        
        def _wrapped_callback(*cb_args: Any, **cb_kwargs: Any) -> Any:
            self._timer_handles.pop(timer_id, None)
            self._metrics.timers_fired += 1
            return callback(*cb_args, **cb_kwargs)
        
        handle = super().call_later(delay, _wrapped_callback, *args, context=context)
        self._timer_handles[timer_id] = handle
        self._metrics.timers_scheduled += 1
        return handle

    def call_at(
        self,
        when: float,
        callback: Callable[..., Any],
        *args: Any,
        context: contextvars.Context | None = None,
    ) -> asyncio.TimerHandle:
        self._timer_counter += 1
        timer_id = self._timer_counter
        
        def _wrapped_callback(*cb_args: Any, **cb_kwargs: Any) -> Any:
            self._timer_handles.pop(timer_id, None)
            self._metrics.timers_fired += 1
            return callback(*cb_args, **cb_kwargs)
        
        handle = super().call_at(when, _wrapped_callback, *args, context=context)
        self._timer_handles[timer_id] = handle
        self._metrics.timers_scheduled += 1
        return handle

    def get_active_timers(self) -> int:
        return len(self._timer_handles)

    # ========================================================================
    # Signal Handling
    # ========================================================================

    def add_signal_handler(
        self,
        sig: signal.Signals | int,
        callback: Callable[..., Any],
        *args: Any,
    ) -> None:
        """Add a signal handler."""
        self._check_closed()
        sig_num = int(sig)

        if not _handle_signals_var.get():
            return

        if sig_num in self._signal_handlers:
            raise ValueError(f"Signal {sig_num} already has a handler")

        if sys.platform != "win32":
            try:
                self._original_signal_handlers[sig_num] = signal.signal(sig_num, signal.SIG_IGN)
            except (OSError, ValueError):
                pass

        self._signal_handlers[sig_num] = functools.partial(callback, *args)
        super().add_signal_handler(sig_num, self._signal_handlers[sig_num])

    def remove_signal_handler(self, sig: signal.Signals | int) -> bool:
        """Remove a signal handler."""
        sig_num = int(sig)
        handler = self._signal_handlers.pop(sig_num, None)

        if handler is None:
            return False

        try:
            super().remove_signal_handler(sig_num)
        except (OSError, ValueError):
            pass

        return True

    def _restore_signal_handlers(self) -> None:
        """Restore original signal handlers."""
        for sig_num, handler in self._original_signal_handlers.items():
            try:
                signal.signal(sig_num, handler)
            except (OSError, ValueError):
                pass
        self._original_signal_handlers.clear()

    # ========================================================================
    # Extensions
    # ========================================================================

    def register_extension(self, name: str, extension: Any) -> None:
        """Register an extension."""
        if name in self._extensions:
            raise ExtensionError(f"Extension '{name}' already registered", extension_name=name)
        self._extensions[name] = extension

        # Call init hook
        if hasattr(extension, "on_loop_init"):
            extension.on_loop_init(self)

        logger.info(f"Registered extension: {name}")

    def get_extension(self, name: str) -> Any:
        """Get an extension by name."""
        if name not in self._extensions:
            raise ExtensionNotFoundError(f"Extension '{name}' not found", extension_name=name)
        return self._extensions[name]

    def unregister_extension(self, name: str) -> None:
        """Unregister an extension."""
        ext = self._extensions.pop(name, None)
        if ext and hasattr(ext, "on_loop_shutdown"):
            ext.on_loop_shutdown(self)

    def add_hook(self, hook_name: str, callback: Callable) -> None:
        """Add a lifecycle hook."""
        self._extension_hooks.setdefault(hook_name, []).append(callback)

    def remove_hook(self, hook_name: str, callback: Callable) -> bool:
        """Remove a lifecycle hook."""
        hooks = self._extension_hooks.get(hook_name, [])
        if callback in hooks:
            hooks.remove(callback)
            return True
        return False

    # ========================================================================
    # Metrics and Debugging
    # ========================================================================

    def _update_memory_metrics(self) -> None:
        """Update memory metrics."""
        try:
            import psutil

            process = psutil.Process(os.getpid())
            mem = process.memory_info().rss / 1024 / 1024
            self._metrics.current_memory_mb = mem
            if mem > self._metrics.peak_memory_mb:
                self._metrics.peak_memory_mb = mem
            self._metrics.open_fds = process.num_fds() if hasattr(process, "num_fds") else 0
        except Exception:
            pass

    def get_slow_callbacks(self, limit: int = 10) -> list[_CallbackTrace]:
        """Get slowest callbacks."""
        sorted_traces = sorted(
            self._callback_traces, key=lambda t: t.duration, reverse=True
        )
        return list(sorted_traces[:limit])

    def get_slow_tasks(self, limit: int = 10) -> list[_TaskTrace]:
        """Get slowest tasks."""
        all_traces = list(self._task_traces.values())
        completed = [t for t in all_traces if t.completed_at is not None]
        sorted_traces = sorted(completed, key=lambda t: t.duration, reverse=True)
        return sorted_traces[:limit]

    def print_debug_info(self) -> None:
        """Print comprehensive debug information."""
        print(f"\n=== DebugEventLoop #{self._loop_id} Debug Info ===")
        print(f"State: {self._state}")
        print(f"Running: {self.is_running}")
        print(f"Thread: {self._thread_id} (current: {threading.get_ident()})")
        print(f"\nMetrics:")
        for k, v in self._metrics.to_dict().items():
            print(f"  {k}: {v}")

        print(f"\nPending Tasks: {len(self._pending_tasks)}")
        for task in self._pending_tasks:
            print(f"  {task.get_name()}: {task.get_coro()}")

        print(f"\nSlow Callbacks (top 5):")
        for trace in self.get_slow_callbacks(5):
            print(f"  {trace.name}: {trace.duration:.4f}s")

        print(f"\nExtensions: {list(self._extensions.keys())}")

    # ========================================================================
    # Context Management
    # ========================================================================

    def __enter__(self) -> DebugEventLoop:
        _loop_context_var.set(self)
        return self

    def __exit__(self, *args: Any) -> None:
        _loop_context_var.set(None)

    @classmethod
    def get_current_loop(cls) -> DebugEventLoop | None:
        """Get the current loop from context."""
        return _loop_context_var.get(None)

    # ========================================================================
    # Abstract Method Implementations
    # ========================================================================

    def _make_self_pipe(self) -> None:
        """Create self-pipe for thread-safe wakeups."""
        super()._make_self_pipe()

    def _process_self_data(self, data: bytes) -> None:
        """Process self-pipe data."""
        super()._process_self_data(data)

    def _write_to_self(self) -> None:
        """Write to self-pipe to wake up loop."""
        super()._write_to_self()

    # These are implemented in BaseEventLoop
    def _add_reader(self, fd: int, callback: Callable, *args: Any) -> None: ...
    def _remove_reader(self, fd: int) -> bool: ...
    def _add_writer(self, fd: int, callback: Callable, *args: Any) -> None: ...
    def _remove_writer(self, fd: int) -> bool: ...

    # Override to add tracing
    def add_reader(self, fd: int, callback: Callable, *args: Any) -> None:
        if self._config.enable_tracing:
            callback = self._trace_callback(callback)
        super().add_reader(fd, callback, *args)

    def add_writer(self, fd: int, callback: Callable, *args: Any) -> None:
        if self._config.enable_tracing:
            callback = self._trace_callback(callback)
        super().add_writer(fd, callback, *args)


def new_event_loop(config: LoopConfig | None = None) -> DebugEventLoop:
    """Create a new DebugEventLoop."""
    return DebugEventLoop(config)


def get_event_loop() -> DebugEventLoop:
    """Get the current event loop, creating one if necessary."""
    loop = DebugEventLoop.get_current_loop()
    if loop is None:
        try:
            loop = asyncio.get_running_loop()
            if not isinstance(loop, DebugEventLoop):
                # Wrap existing loop? For now create new
                loop = new_event_loop()
        except RuntimeError:
            loop = new_event_loop()
    return loop


def set_event_loop(loop: DebugEventLoop | None) -> None:
    """Set the current event loop."""
    if loop is not None:
        _loop_context_var.set(loop)
    else:
        _loop_context_var.set(None)