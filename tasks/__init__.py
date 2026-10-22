"""
Advanced Task Management

Provides enhanced task primitives including:
- TaskGroup with structured concurrency
- Task supervision and monitoring
- Cancellation scopes
- Task-local storage
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import inspect
import sys
import threading
import time
import warnings
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from ..core.exceptions import TaskCancelledError, TaskError, TaskTimeoutError
from ..core.loop import DebugEventLoop, get_event_loop

if sys.version_info >= (3, 11):
    from asyncio import TaskGroup as _StdTaskGroup
else:
    from asyncio import TaskGroup as _StdTaskGroup

T = TypeVar("T")

_task_local_storage: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar(
    "aevum_task_local", default={}
)
_current_task_scope: contextvars.ContextVar["TaskScope | None"] = contextvars.ContextVar(
    "aevum_current_task_scope", default=None
)


@dataclass(slots=True)
class TaskInfo:
    """Information about a task."""

    name: str
    coro: Coroutine[Any, Any, Any]
    created_at: float
    created_stack: list[str]
    started_at: float | None = None
    completed_at: float | None = None
    duration: float = 0.0
    cancelled: bool = False
    exception: BaseException | None = None
    result: Any = None

    @property
    def is_done(self) -> bool:
        return self.completed_at is not None

    @property
    def is_slow(self, threshold: float = 1.0) -> bool:
        return self.duration > threshold


class TaskScope:
    """
    Structured concurrency scope for managing task lifecycles.

    Features:
    - Automatic cleanup on exit
    - Error aggregation
    - Timeout propagation
    - Graceful cancellation
    """

    def __init__(
        self,
        *,
        loop: DebugEventLoop | None = None,
        timeout: float | None = None,
        name: str = "TaskScope",
        shield: bool = False,
    ):
        self._loop = loop or get_event_loop()
        self._timeout = timeout
        self._name = name
        self._shield = shield
        self._tasks: set[asyncio.Task] = set()
        self._entered = False
        self._exceptions: list[BaseException] = []
        self._results: list[Any] = []
        self._parent_scope = _current_task_scope.get(None)
        self._timeout_handle: asyncio.TimerHandle | None = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def tasks(self) -> set[asyncio.Task]:
        return self._tasks

    @property
    def parent_scope(self) -> TaskScope | None:
        return self._parent_scope

    @property
    def is_active(self) -> bool:
        return self._entered and not self._tasks_done()

    def _tasks_done(self) -> bool:
        return all(t.done() for t in self._tasks)

    async def __aenter__(self) -> TaskScope:
        self._entered = True
        _current_task_scope.set(self)

        if self._timeout is not None:
            self._timeout_handle = self._loop.call_later(self._timeout, self._on_timeout)

        return self

    async def __aexit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        self._entered = False
        _current_task_scope.set(self._parent_scope)

        if self._timeout_handle:
            self._timeout_handle.cancel()
            self._timeout_handle = None

        await self._cancel_all()

        if exc_val is not None:
            return False

        if self._exceptions:
            # Raise first exception
            raise self._exceptions[0]

        return False

    def _on_timeout(self) -> None:
        """Handle scope timeout."""
        for task in self._tasks:
            if not task.done():
                task.cancel(msg=f"TaskScope '{self._name}' timed out after {self._timeout}s")

    def create_task(
        self,
        coro: Awaitable[T],
        *,
        name: str | None = None,
        shield: bool = False,
    ) -> asyncio.Task[T]:
        """Create a task within this scope."""
        if not self._entered:
            raise RuntimeError("TaskScope not entered. Use 'async with' or call __aenter__ first.")

        task = self._loop.create_task(coro, name=name)
        self._tasks.add(task)

        def _done(t: asyncio.Task) -> None:
            self._tasks.discard(t)
            if t.cancelled():
                return
            if (exc := t.exception()) is not None:
                self._exceptions.append(exc)
            else:
                self._results.append(t.result())

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
        """Wait for all tasks to complete and return results."""
        if not self._tasks:
            return []
        results = await asyncio.gather(*self._tasks, return_exceptions=True)
        return results

    async def wait_any(self) -> tuple[asyncio.Task, Any]:
        """Wait for any task to complete."""
        if not self._tasks:
            raise RuntimeError("No tasks in scope")

        done, _ = await asyncio.wait(
            self._tasks, return_when=asyncio.FIRST_COMPLETED
        )
        task = done.pop()
        return task, task.result() if not task.cancelled() else None

    def get_task_info(self) -> list[TaskInfo]:
        """Get information about all tasks in this scope."""
        infos = []
        for task in self._tasks:
            info = TaskInfo(
                name=task.get_name(),
                coro=task.get_coro(),
                created_at=time.monotonic(),  # Approximate
                created_stack=[],
            )
            if task.done():
                info.completed_at = time.monotonic()
                info.cancelled = task.cancelled()
                info.exception = task.exception()
                if not info.cancelled and info.exception is None:
                    info.result = task.result()
            infos.append(info)
        return infos


class SupervisedTaskGroup:
    """
    Task group with supervision capabilities.

    Features:
    - Automatic restart on failure (configurable)
    - Health monitoring
    - Lifecycle callbacks
    """

    def __init__(
        self,
        *,
        loop: DebugEventLoop | None = None,
        name: str = "SupervisedTaskGroup",
        restart_on_failure: bool = False,
        max_restarts: int = 3,
        restart_delay: float = 1.0,
        on_task_start: Callable[[asyncio.Task], None] | None = None,
        on_task_done: Callable[[asyncio.Task], None] | None = None,
        on_task_error: Callable[[asyncio.Task, BaseException], None] | None = None,
    ):
        self._loop = loop or get_event_loop()
        self._name = name
        self._restart_on_failure = restart_on_failure
        self._max_restarts = max_restarts
        self._restart_delay = restart_delay
        self._on_task_start = on_task_start
        self._on_task_done = on_task_done
        self._on_task_error = on_task_error
        self._tasks: dict[str, asyncio.Task] = {}
        self._restart_counts: dict[str, int] = {}
        self._running = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def tasks(self) -> dict[str, asyncio.Task]:
        return self._tasks.copy()

    async def __aenter__(self) -> SupervisedTaskGroup:
        self._running = True
        return self

    async def __aexit__(self, *args: Any) -> None:
        self._running = False
        await self.cancel_all()

    def create_task(
        self,
        coro: Awaitable[T],
        *,
        name: str | None = None,
        restart: bool | None = None,
    ) -> asyncio.Task[T]:
        """Create a supervised task."""
        if not self._running:
            raise RuntimeError("Task group not running")

        task_name = name or f"task-{len(self._tasks)}"
        if task_name in self._tasks:
            raise ValueError(f"Task '{task_name}' already exists in group")

        restart_enabled = restart if restart is not None else self._restart_on_failure

        async def _wrapped() -> T:
            task = asyncio.current_task()
            assert task is not None
            task.set_name(task_name)

            if self._on_task_start:
                self._on_task_start(task)

            restarts = 0
            while True:
                try:
                    result = await coro
                    if self._on_task_done:
                        self._on_task_done(task)
                    return result
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    if self._on_task_error:
                        self._on_task_error(task, e)

                    if not restart_enabled or restarts >= self._max_restarts:
                        raise

                    restarts += 1
                    self._restart_counts[task_name] = restarts
                    await asyncio.sleep(self._restart_delay * restarts)

        task = self._loop.create_task(_wrapped(), name=task_name)
        self._tasks[task_name] = task
        return task

    async def cancel_all(self) -> None:
        """Cancel all tasks in the group."""
        for task in self._tasks.values():
            if not task.done():
                task.cancel()

        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        self._tasks.clear()

    async def wait_all(self) -> dict[str, Any]:
        """Wait for all tasks and return results."""
        results = {}
        for name, task in self._tasks.items():
            try:
                results[name] = await task
            except Exception as e:
                results[name] = e
        return results


class CancellationScope:
    """
    Explicit cancellation scope for fine-grained control.

    Unlike TaskScope, this allows manual cancellation control
    and can be nested.
    """

    def __init__(self, *, shield: bool = False):
        self._shield = shield
        self._cancelled = False
        self._cancel_called = False
        self._children: list[CancellationScope] = []
        self._parent = _cancellation_scope_var.get(None)
        if self._parent:
            self._parent._children.append(self)

    @property
    def cancelled(self) -> bool:
        return self._cancelled or (self._parent and self._parent.cancelled)

    @property
    def shield(self) -> bool:
        return self._shield

    def cancel(self) -> None:
        """Cancel this scope and all children."""
        if self._cancel_called:
            return
        self._cancel_called = True
        self._cancelled = True
        for child in self._children:
            child.cancel()

    def __enter__(self) -> CancellationScope:
        _cancellation_scope_var.set(self)
        return self

    def __exit__(self, *args: Any) -> None:
        _cancellation_scope_var.set(self._parent)

    async def __aenter__(self) -> CancellationScope:
        return self.__enter__()

    async def __aexit__(self, *args: Any) -> None:
        self.__exit__(*args)


_cancellation_scope_var: contextvars.ContextVar[CancellationScope | None] = contextvars.ContextVar(
    "aevum_cancellation_scope", default=None
)


def get_cancellation_scope() -> CancellationScope | None:
    """Get the current cancellation scope."""
    return _cancellation_scope_var.get(None)


def set_task_local(key: str, value: Any) -> None:
    """Set a task-local value."""
    storage = _task_local_storage.get({})
    storage[key] = value
    _task_local_storage.set(storage)


def get_task_local(key: str, default: Any = None) -> Any:
    """Get a task-local value."""
    return _task_local_storage.get({}).get(key, default)


def clear_task_local() -> None:
    """Clear all task-local values."""
    _task_local_storage.set({})


@asynccontextmanager
async def task_scope(
    *,
    loop: DebugEventLoop | None = None,
    timeout: float | None = None,
    name: str = "TaskScope",
) -> AsyncIterator[TaskScope]:
    """Create a task scope as async context manager."""
    scope = TaskScope(loop=loop, timeout=timeout, name=name)
    async with scope:
        yield scope


@asynccontextmanager
async def supervised_task_group(
    *,
    loop: DebugEventLoop | None = None,
    name: str = "SupervisedTaskGroup",
    restart_on_failure: bool = False,
    max_restarts: int = 3,
    restart_delay: float = 1.0,
) -> AsyncIterator[SupervisedTaskGroup]:
    """Create a supervised task group."""
    group = SupervisedTaskGroup(
        loop=loop,
        name=name,
        restart_on_failure=restart_on_failure,
        max_restarts=max_restarts,
        restart_delay=restart_delay,
    )
    async with group:
        yield group


async def gather_with_concurrency(
    *tasks: Awaitable[T],
    max_concurrent: int,
    loop: DebugEventLoop | None = None,
) -> list[T]:
    """
    Gather tasks with concurrency limit.

    Unlike asyncio.gather, this limits the number of concurrently running tasks.
    """
    loop = loop or get_event_loop()
    semaphore = asyncio.Semaphore(max_concurrent)

    async def _limited(task: Awaitable[T]) -> T:
        async with semaphore:
            return await task

    return await asyncio.gather(*[_limited(t) for t in tasks])


async def wait_for_first(
    *tasks: Awaitable[T],
    loop: DebugEventLoop | None = None,
) -> tuple[int, T]:
    """
    Wait for the first task to complete.

    Returns (index, result) of the first completed task.
    """
    loop = loop or get_event_loop()
    done, pending = await asyncio.wait(
        [asyncio.ensure_future(t, loop=loop) for t in tasks],
        return_when=asyncio.FIRST_COMPLETED,
    )
    for i, task in enumerate(tasks):
        if task in done:
            return i, task.result()
    raise RuntimeError("No tasks completed")


async def wait_for_all(
    *tasks: Awaitable[T],
    loop: DebugEventLoop | None = None,
    return_exceptions: bool = False,
) -> list[T]:
    """Wait for all tasks to complete."""
    loop = loop or get_event_loop()
    return await asyncio.gather(*tasks, return_exceptions=return_exceptions)


class TaskMonitor:
    """
    Monitor for tracking task lifecycle events.

    Can be used to build custom observability tools.
    """

    def __init__(self):
        self._events: list[dict] = []
        self._max_events = 10000

    def on_task_created(self, task: asyncio.Task) -> None:
        """Called when a task is created."""
        self._record_event("created", task)

    def on_task_started(self, task: asyncio.Task) -> None:
        """Called when a task starts running."""
        self._record_event("started", task)

    def on_task_done(self, task: asyncio.Task) -> None:
        """Called when a task completes."""
        self._record_event("done", task)

    def on_task_cancelled(self, task: asyncio.Task) -> None:
        """Called when a task is cancelled."""
        self._record_event("cancelled", task)

    def _record_event(self, event_type: str, task: asyncio.Task) -> None:
        self._events.append({
            "type": event_type,
            "task_name": task.get_name(),
            "timestamp": time.monotonic(),
        })
        if len(self._events) > self._max_events:
            self._events.pop(0)

    def get_events(self, event_type: str | None = None) -> list[dict]:
        """Get recorded events."""
        if event_type:
            return [e for e in self._events if e["type"] == event_type]
        return self._events.copy()

    def clear(self) -> None:
        """Clear recorded events."""
        self._events.clear()


# Default monitor instance
default_monitor = TaskMonitor()


def enable_task_monitoring(monitor: TaskMonitor | None = None) -> TaskMonitor:
    """Enable global task monitoring."""
    global default_monitor
    if monitor is not None:
        default_monitor = monitor
    return default_monitor


# Alias for backward compatibility
create_task_scope = task_scope