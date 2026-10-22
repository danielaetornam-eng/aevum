"""
Loop Context Management

Provides context managers and utilities for event loop lifecycle management.
"""

from __future__ import annotations

import asyncio
import contextvars
import threading
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, TypeVar

from .config import LoopConfig
from ._context_vars import _loop_context_var, current_loop, set_current_loop

if TYPE_CHECKING:
    from .loop import DebugEventLoop

# Avoid circular import
def get_event_loop() -> "DebugEventLoop":
    from .loop import get_event_loop as _get_event_loop
    return _get_event_loop()

def new_event_loop(config: LoopConfig | None = None) -> "DebugEventLoop":
    from .loop import new_event_loop as _new_event_loop
    return _new_event_loop(config)

T = TypeVar("T")


class LoopContext:
    """
    Context manager for event loop lifecycle.

    Provides a clean way to manage loop creation, execution, and cleanup.
    """

    def __init__(
        self,
        loop: DebugEventLoop | None = None,
        config: LoopConfig | None = None,
        *,
        set_as_current: bool = True,
        run_forever: bool = False,
    ):
        self._loop = loop
        self._config = config
        self._set_as_current = set_as_current
        self._run_forever = run_forever
        self._previous_loop: DebugEventLoop | None = None
        self._owns_loop = False

    @property
    def loop(self) -> DebugEventLoop:
        if self._loop is None:
            self._loop = new_event_loop(self._config)
            self._owns_loop = True
        return self._loop

    def __enter__(self) -> DebugEventLoop:
        loop = self.loop
        if self._set_as_current:
            self._previous_loop = _loop_context_var.get(None)
            set_event_loop(loop)
        return loop

    def __exit__(self, exc_type: type | None, exc_val: BaseException | None, exc_tb: Any) -> None:
        if self._set_as_current:
            if self._previous_loop is not None:
                set_event_loop(self._previous_loop)
            else:
                _loop_context_var.set(None)

        if self._owns_loop and not self._loop.is_closed:
            if self._run_forever:
                self._loop.run_forever()
            self._loop.close()

    async def __aenter__(self) -> DebugEventLoop:
        return self.__enter__()

    async def __aexit__(self, *args: Any) -> None:
        self.__exit__(*args)


@contextmanager
def loop_context(
    loop: DebugEventLoop | None = None,
    config: LoopConfig | None = None,
    *,
    set_as_current: bool = True,
    run_forever: bool = False,
) -> AsyncIterator[DebugEventLoop]:
    """
    Context manager for event loop lifecycle.

    Usage:
        with loop_context() as loop:
            loop.run_until_complete(main())
    """
    ctx = LoopContext(loop, config, set_as_current=set_as_current, run_forever=run_forever)
    try:
        yield ctx.__enter__()
    finally:
        ctx.__exit__(None, None, None)


@asynccontextmanager
async def async_loop_context(
    loop: DebugEventLoop | None = None,
    config: LoopConfig | None = None,
    *,
    set_as_current: bool = True,
) -> AsyncIterator[DebugEventLoop]:
    """
    Async context manager for event loop lifecycle.

    Usage:
        async with async_loop_context() as loop:
            await main()
    """
    ctx = LoopContext(loop, config, set_as_current=set_as_current)
    try:
        yield ctx.__enter__()
    finally:
        await ctx.__aexit__(None, None, None)


def current_loop() -> DebugEventLoop | None:
    """Get the current event loop from context."""
    return _loop_context_var.get(None)


def set_current_loop(loop: DebugEventLoop | None) -> None:
    """Set the current event loop in context."""
    if loop is not None:
        _loop_context_var.set(loop)
    else:
        _loop_context_var.set(None)


def run_in_loop(
    coro: Awaitable[T],
    *,
    loop: DebugEventLoop | None = None,
    config: LoopConfig | None = None,
) -> T:
    """
    Run a coroutine in an event loop, creating one if needed.

    This is a convenience function for running async code from sync code.
    """
    ctx_loop = loop or get_event_loop()
    if ctx_loop.is_running:
        # Loop is already running, create a new one for this coroutine
        with loop_context(config=config) as new_loop:
            return new_loop.run_until_complete(coro)
    return ctx_loop.run_until_complete(coro)


async def run_in_executor(
    func: Callable[..., T],
    *args: Any,
    loop: DebugEventLoop | None = None,
    executor: str | None = None,
) -> T:
    """Run a function in the default executor."""
    loop = loop or get_event_loop()
    return await loop.run_in_executor(None, func, *args)


class AsyncExitStack:
    """Async version of contextlib.ExitStack for managing multiple async context managers."""

    def __init__(self):
        self._stack: list[Callable[[], Awaitable[None]]] = []

    async def __aenter__(self) -> AsyncExitStack:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()

    async def enter_async_context(self, cm: AsyncIterator[T]) -> T:
        """Enter an async context manager."""
        exit_func = cm.__aexit__
        self._stack.append(exit_func)
        return await cm.__aenter__()

    def enter_context(self, cm: Any) -> Any:
        """Enter a sync context manager."""
        exit_func = cm.__exit__
        self._stack.append(lambda *a: exit_func(*a))
        return cm.__enter__()

    async def aclose(self) -> None:
        """Close all context managers in reverse order."""
        for exit_func in reversed(self._stack):
            try:
                await exit_func(None, None, None)
            except Exception:
                pass


@asynccontextmanager
async def timeout(seconds: float | None) -> AsyncIterator[None]:
    """Async context manager for timeouts."""
    loop = get_event_loop()
    if seconds is None:
        yield
        return

    async def _timeout():
        await asyncio.sleep(seconds)
        raise TimeoutError(f"Operation timed out after {seconds}s")

    timeout_task = loop.create_task(_timeout())
    try:
        yield
    finally:
        timeout_task.cancel()
        try:
            await timeout_task
        except asyncio.CancelledError:
            pass


class TaskScope:
    """
    Structured concurrency scope for managing task lifecycles.

    Similar to asyncio.TaskGroup but with additional features:
    - Automatic timeout propagation
    - Error aggregation
    - Graceful cancellation
    """

    def __init__(
        self,
        *,
        loop: DebugEventLoop | None = None,
        timeout: float | None = None,
        name: str = "TaskScope",
    ):
        self._loop = loop or get_event_loop()
        self._timeout = timeout
        self._name = name
        self._tasks: set[asyncio.Task] = set()
        self._entered = False
        self._exceptions: list[BaseException] = []

    @property
    def tasks(self) -> set[asyncio.Task]:
        return self._tasks

    async def __aenter__(self) -> TaskScope:
        self._entered = True
        return self

    async def __aexit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        self._entered = False
        await self._cancel_all()
        if self._exceptions:
            # Raise first exception
            raise self._exceptions[0]
        return False

    def create_task(
        self,
        coro: Awaitable[T],
        *,
        name: str | None = None,
    ) -> asyncio.Task[T]:
        """Create a task within this scope."""
        if not self._entered:
            raise RuntimeError("TaskScope not entered")

        task = self._loop.create_task(coro, name=name)
        self._tasks.add(task)

        def _done(t: asyncio.Task) -> None:
            self._tasks.discard(t)
            if t.cancelled():
                return
            if (exc := t.exception()) is not None:
                self._exceptions.append(exc)

        task.add_done_callback(_done)
        return task

    async def _cancel_all(self) -> None:
        """Cancel all tasks in the scope."""
        for task in self._tasks:
            if not task.done():
                task.cancel()

        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    async def wait_all(self) -> list[Any]:
        """Wait for all tasks to complete."""
        if not self._tasks:
            return []
        return await asyncio.gather(*self._tasks, return_exceptions=True)

    async def wait_any(self) -> tuple[asyncio.Task, Any]:
        """Wait for any task to complete."""
        if not self._tasks:
            raise RuntimeError("No tasks in scope")

        done, pending = await asyncio.wait(
            self._tasks, return_when=asyncio.FIRST_COMPLETED
        )
        task = done.pop()
        return task, task.result() if not task.cancelled() else None


def create_task_scope(
    *,
    loop: DebugEventLoop | None = None,
    timeout: float | None = None,
    name: str = "TaskScope",
) -> TaskScope:
    """Create a new task scope."""
    return TaskScope(loop=loop, timeout=timeout, name=name)