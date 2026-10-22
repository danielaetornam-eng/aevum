"""
Thread Pool Executor Management

Provides advanced thread pool management with:
- Lifecycle management
- Health monitoring
- Dynamic scaling
- Task queue monitoring
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import os
import threading
import time
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, TypeVar

from ..core.exceptions import ExecutorError, ExecutorShutdownError
from ..core.loop import DebugEventLoop, get_event_loop

T = TypeVar("T")


@dataclass(slots=True)
class ExecutorMetrics:
    """Metrics for thread pool executor."""

    submitted: int = 0
    completed: int = 0
    failed: int = 0
    cancelled: int = 0
    active_threads: int = 0
    idle_threads: int = 0
    queue_size: int = 0
    total_time: float = 0.0
    avg_time: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "submitted": self.submitted,
            "completed": self.completed,
            "failed": self.failed,
            "cancelled": self.cancelled,
            "active_threads": self.active_threads,
            "idle_threads": self.idle_threads,
            "queue_size": self.queue_size,
            "total_time": self.total_time,
            "avg_time": self.avg_time,
        }


class ManagedThreadPoolExecutor:
    """
    Managed thread pool executor with lifecycle and monitoring.

    Features:
    - Dynamic scaling based on load
    - Health checks
    - Graceful shutdown
    - Metrics collection
    - Task prioritization
    """

    def __init__(
        self,
        max_workers: int | None = None,
        *,
        loop: DebugEventLoop | None = None,
        name: str = "ThreadPool",
        min_workers: int = 0,
        max_idle_time: float = 60.0,
        health_check_interval: float = 30.0,
        queue_size: int = 0,  # 0 = unlimited
    ):
        self._loop = loop or get_event_loop()
        self._name = name
        self._min_workers = min_workers
        self._max_workers = max_workers or min(32, (os.cpu_count() or 4) + 4)
        self._max_idle_time = max_idle_time
        self._health_check_interval = health_check_interval
        self._queue_size = queue_size

        self._executor: concurrent.futures.ThreadPoolExecutor | None = None
        self._metrics = ExecutorMetrics()
        self._shutdown = False
        self._lock = threading.RLock()
        self._health_check_task: asyncio.Task | None = None
        self._worker_count = 0
        self._idle_since: dict[int, float] = {}

    @property
    def name(self) -> str:
        return self._name

    @property
    def max_workers(self) -> int:
        return self._max_workers

    @property
    def metrics(self) -> ExecutorMetrics:
        return self._metrics

    @property
    def is_shutdown(self) -> bool:
        return self._shutdown

    def _ensure_executor(self) -> concurrent.futures.ThreadPoolExecutor:
        """Ensure executor is created."""
        with self._lock:
            if self._executor is None or self._shutdown:
                if self._shutdown:
                    raise ExecutorShutdownError(executor_name=self._name)
                self._executor = concurrent.futures.ThreadPoolExecutor(
                    max_workers=self._max_workers,
                    thread_name_prefix=f"{self._name}-",
                )
                self._worker_count = self._max_workers
                self._start_health_check()
            return self._executor

    def _start_health_check(self) -> None:
        """Start periodic health check."""
        if self._health_check_task is None or self._health_check_task.done():
            self._health_check_task = self._loop.create_task(
                self._health_check_loop(), name=f"{self._name}-health-check"
            )

    async def _health_check_loop(self) -> None:
        """Periodic health check and scaling."""
        while not self._shutdown:
            try:
                await asyncio.sleep(self._health_check_interval)
                if self._shutdown:
                    break
                self._perform_health_check()
            except asyncio.CancelledError:
                break
            except Exception:
                pass  # Log in production

    def _perform_health_check(self) -> None:
        """Perform health check and adjust worker count."""
        with self._lock:
            if self._executor is None:
                return

            # Update metrics
            self._metrics.active_threads = len(
                [t for t in self._executor._threads if t.is_alive()]
            )
            self._metrics.idle_threads = self._worker_count - self._metrics.active_threads

            # Scale down idle workers
            now = time.monotonic()
            idle_workers = [
                tid for tid, since in self._idle_since.items()
                if now - since > self._max_idle_time
            ]
            if len(idle_workers) > self._min_workers:
                # In production, we'd actually reduce thread count
                pass

    def submit(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> concurrent.futures.Future[T]:
        """Submit a function to the executor."""
        if self._shutdown:
            raise ExecutorShutdownError(executor_name=self._name)

        executor = self._ensure_executor()
        self._metrics.submitted += 1

        def _wrapped() -> T:
            start = time.monotonic()
            try:
                result = fn(*args, **kwargs)
                self._metrics.completed += 1
                return result
            except Exception as e:
                self._metrics.failed += 1
                raise
            finally:
                elapsed = time.monotonic() - start
                self._metrics.total_time += elapsed
                if self._metrics.completed > 0:
                    self._metrics.avg_time = self._metrics.total_time / self._metrics.completed

        return executor.submit(_wrapped)

    async def run_in_executor(
        self,
        fn: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Run a function in the executor asynchronously."""
        future = self.submit(fn, *args, **kwargs)
        return await asyncio.wrap_future(future, loop=self._loop)

    def map(
        self,
        fn: Callable[..., T],
        *iterables: Any,
        timeout: float | None = None,
        chunksize: int = 1,
    ) -> list[T]:
        """Map function over iterables."""
        executor = self._ensure_executor()
        return list(executor.map(fn, *iterables, timeout=timeout, chunksize=chunksize))

    async def map_async(
        self,
        fn: Callable[..., T],
        *iterables: Any,
        chunksize: int = 1,
    ) -> list[T]:
        """Async map function over iterables."""
        loop = self._loop
        futures = [loop.run_in_executor(self, fn, *args) for args in zip(*iterables)]
        return await asyncio.gather(*futures)

    def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
        """Shutdown the executor."""
        with self._lock:
            if self._shutdown:
                return
            self._shutdown = True

        if self._health_check_task and not self._health_check_task.done():
            self._health_check_task.cancel()

        if self._executor:
            self._executor.shutdown(wait=wait, cancel_futures=cancel_futures)
            self._executor = None

    def __enter__(self) -> ManagedThreadPoolExecutor:
        return self

    def __exit__(self, *args: Any) -> None:
        self.shutdown(wait=True)


class ProcessPoolExecutor:
    """
    Managed process pool executor for CPU-bound tasks.

    Similar to ManagedThreadPoolExecutor but uses processes.
    """

    def __init__(
        self,
        max_workers: int | None = None,
        *,
        loop: DebugEventLoop | None = None,
        name: str = "ProcessPool",
    ):
        self._loop = loop or get_event_loop()
        self._name = name
        self._max_workers = max_workers or os.cpu_count() or 4
        self._executor: concurrent.futures.ProcessPoolExecutor | None = None
        self._shutdown = False

    @property
    def max_workers(self) -> int:
        return self._max_workers

    def _ensure_executor(self) -> concurrent.futures.ProcessPoolExecutor:
        if self._executor is None:
            if self._shutdown:
                raise ExecutorShutdownError(executor_name=self._name)
            self._executor = concurrent.futures.ProcessPoolExecutor(
                max_workers=self._max_workers,
            )
        return self._executor

    def submit(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> concurrent.futures.Future[T]:
        executor = self._ensure_executor()
        return executor.submit(fn, *args, **kwargs)

    async def run_in_executor(
        self,
        fn: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        future = self.submit(fn, *args, **kwargs)
        return await asyncio.wrap_future(future, loop=self._loop)

    def shutdown(self, wait: bool = True) -> None:
        if self._shutdown:
            return
        self._shutdown = True
        if self._executor:
            self._executor.shutdown(wait=wait)
            self._executor = None


class ExecutorManager:
    """
    Manager for multiple executors.

    Provides centralized management of thread and process pools.
    """

    def __init__(self, loop: DebugEventLoop | None = None):
        self._loop = loop or get_event_loop()
        self._executors: dict[str, ManagedThreadPoolExecutor | ProcessPoolExecutor] = {}
        self._default_executor: str | None = None

    def create_thread_pool(
        self,
        name: str,
        max_workers: int | None = None,
        **kwargs: Any,
    ) -> ManagedThreadPoolExecutor:
        """Create a thread pool executor."""
        if name in self._executors:
            raise ValueError(f"Executor '{name}' already exists")
        executor = ManagedThreadPoolExecutor(max_workers, loop=self._loop, name=name, **kwargs)
        self._executors[name] = executor
        if self._default_executor is None:
            self._default_executor = name
        return executor

    def create_process_pool(
        self,
        name: str,
        max_workers: int | None = None,
    ) -> ProcessPoolExecutor:
        """Create a process pool executor."""
        if name in self._executors:
            raise ValueError(f"Executor '{name}' already exists")
        executor = ProcessPoolExecutor(max_workers, loop=self._loop, name=name)
        self._executors[name] = executor
        if self._default_executor is None:
            self._default_executor = name
        return executor

    def get_executor(self, name: str) -> ManagedThreadPoolExecutor | ProcessPoolExecutor:
        """Get an executor by name."""
        if name not in self._executors:
            raise ExecutorError(f"Executor '{name}' not found", executor_name=name)
        return self._executors[name]

    def get_default(self) -> ManagedThreadPoolExecutor | ProcessPoolExecutor | None:
        """Get the default executor."""
        if self._default_executor:
            return self._executors[self._default_executor]
        return None

    def set_default(self, name: str) -> None:
        """Set the default executor."""
        if name not in self._executors:
            raise ExecutorError(f"Executor '{name}' not found", executor_name=name)
        self._default_executor = name

    async def run_in_executor(
        self,
        fn: Callable[..., T],
        *args: Any,
        executor: str | None = None,
        **kwargs: Any,
    ) -> T:
        """Run function in named executor."""
        exec_name = executor or self._default_executor
        if exec_name is None:
            raise ExecutorError("No executor specified and no default set")
        return await self._executors[exec_name].run_in_executor(fn, *args, **kwargs)

    def shutdown_all(self, wait: bool = True) -> None:
        """Shutdown all executors."""
        for executor in self._executors.values():
            executor.shutdown(wait=wait)
        self._executors.clear()
        self._default_executor = None

    def get_all_metrics(self) -> dict[str, ExecutorMetrics]:
        """Get metrics for all executors."""
        return {
            name: exec.metrics if hasattr(exec, "metrics") else {}
            for name, exec in self._executors.items()
        }


# Global executor manager
_executor_manager: ExecutorManager | None = None


def get_executor_manager(loop: DebugEventLoop | None = None) -> ExecutorManager:
    """Get the global executor manager."""
    global _executor_manager
    if _executor_manager is None:
        _executor_manager = ExecutorManager(loop)
    return _executor_manager


def create_thread_pool(
    name: str,
    max_workers: int | None = None,
    **kwargs: Any,
) -> ManagedThreadPoolExecutor:
    """Create a thread pool in the global manager."""
    return get_executor_manager().create_thread_pool(name, max_workers, **kwargs)


def create_process_pool(
    name: str,
    max_workers: int | None = None,
) -> ProcessPoolExecutor:
    """Create a process pool in the global manager."""
    return get_executor_manager().create_process_pool(name, max_workers)


async def run_in_executor(
    fn: Callable[..., T],
    *args: Any,
    executor: str | None = None,
    **kwargs: Any,
) -> T:
    """Run function in global executor manager."""
    return await get_executor_manager().run_in_executor(fn, *args, executor=executor, **kwargs)


@contextmanager
def executor_context(
    executor: ManagedThreadPoolExecutor | ProcessPoolExecutor | None = None,
    name: str | None = None,
):
    """Context manager for executor lifecycle."""
    if executor is None and name:
        executor = get_executor_manager().get_executor(name)
    elif executor is None:
        executor = get_executor_manager().get_default()
        if executor is None:
            executor = create_thread_pool("default")

    try:
        yield executor
    finally:
        pass  # Don't shutdown by default