"""
Example: Advanced Async Patterns with Aevum

Demonstrates advanced patterns for building robust async applications:
- Structured concurrency with supervision
- Resource management
- Background job processing
- Real-time event processing
"""

from __future__ import annotations

import asyncio
import random
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Awaitable

from aevum import (
    # Core
    DebugEventLoop,
    LoopConfig,
    LoopContext,
    # Tasks
    TaskScope,
    SupervisedTaskGroup,
    CancellationScope,
    create_task_scope,
    supervised_task_group,
    gather_with_concurrency,
    wait_for_first,
    set_task_local,
    get_task_local,
    # Executors
    ManagedThreadPoolExecutor,
    ProcessPoolExecutor,
    create_thread_pool,
    run_in_executor,
    # Subprocess
    AsyncProcess,
    ProcessGroup,
    run_command_streaming,
    # Signals
    GracefulShutdown,
    wait_for_shutdown,
    # Observability
    trace,
    increment,
    gauge,
    histogram,
    timing,
    measure_time,
    get_tracer,
    get_metrics,
    HealthCheckRegistry,
    run_health_checks,
    # Extensions
    Extension,
    ExtensionMetadata,
    Application,
    application_context,
)
from aevum.core import get_event_loop


# ============================================================================
# Job Queue System
# ============================================================================

@dataclass
class Job:
    """Background job."""
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    name: str = ""
    payload: dict = field(default_factory=dict)
    priority: int = 0
    retries: int = 3
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    completed_at: float | None = None
    result: Any = None
    error: str | None = None


class JobQueue:
    """Priority job queue with workers."""

    def __init__(self, max_workers: int = 4):
        self._queue: asyncio.PriorityQueue[tuple[int, Job]] = asyncio.PriorityQueue()
        self._workers: list[asyncio.Task] = []
        self._max_workers = max_workers
        self._running = False
        self._executor = create_thread_pool("job-workers", max_workers=max_workers)
        self._handlers: dict[str, Callable[[Job], Awaitable[Any]]] = {}

    def register_handler(self, job_name: str, handler: Callable[[Job], Awaitable[Any]]) -> None:
        """Register a job handler."""
        self._handlers[job_name] = handler

    async def enqueue(self, job: Job) -> None:
        """Add job to queue."""
        # Negative priority for max-heap behavior
        await self._queue.put((-job.priority, job))
        increment("jobs_queued", tags={"job": job.name})

    async def start(self) -> None:
        """Start worker tasks."""
        self._running = True
        for i in range(self._max_workers):
            task = asyncio.create_task(self._worker(i), name=f"job-worker-{i}")
            self._workers.append(task)

    async def stop(self) -> None:
        """Stop workers."""
        self._running = False
        for task in self._workers:
            task.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._executor.shutdown(wait=True)

    async def _worker(self, worker_id: int) -> None:
        """Worker loop."""
        while self._running:
            try:
                _, job = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            await self._process_job(job)

    @trace("job_queue.process_job")
    async def _process_job(self, job: Job) -> None:
        """Process a single job."""
        job.started_at = time.time()
        increment("jobs_started", tags={"job": job.name})

        handler = self._handlers.get(job.name)
        if not handler:
            job.error = f"No handler for job: {job.name}"
            increment("jobs_failed", tags={"job": job.name, "reason": "no_handler"})
            return

        # Set job context for tracing
        set_task_local("job_id", job.id)
        set_task_local("job_name", job.name)

        try:
            with measure_time("job_duration", tags={"job": job.name}):
                job.result = await handler(job)

            job.completed_at = time.time()
            increment("jobs_completed", tags={"job": job.name})
            histogram("job_duration_seconds", job.completed_at - job.started_at, tags={"job": job.name})

        except Exception as e:
            job.error = str(e)
            job.completed_at = time.time()
            increment("jobs_failed", tags={"job": job.name, "reason": type(e).__name__})
            raise
        finally:
            gauge("queue_size", self._queue.qsize())


# ============================================================================
# Event Stream Processor
# ============================================================================

@dataclass
class Event:
    """Event in stream."""
    type: str
    data: dict
    timestamp: float = field(default_factory=time.time)
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])


class EventStream:
    """Processes event streams with backpressure handling."""

    def __init__(self, buffer_size: int = 10000):
        self._buffer: asyncio.Queue[Event] = asyncio.Queue(maxsize=buffer_size)
        self._processors: list[Callable[[Event], Awaitable[None]]] = []
        self._running = False
        self._processor_task: asyncio.Task | None = None

    def add_processor(self, processor: Callable[[Event], Awaitable[None]]) -> None:
        """Add event processor."""
        self._processors.append(processor)

    async def publish(self, event: Event) -> bool:
        """Publish event to stream."""
        try:
            self._buffer.put_nowait(event)
            increment("events_published", tags={"type": event.type})
            return True
        except asyncio.QueueFull:
            increment("events_dropped", tags={"type": event.type})
            return False

    async def start(self) -> None:
        """Start processing."""
        self._running = True
        self._processor_task = asyncio.create_task(self._process_loop(), name="event-processor")

    async def stop(self) -> None:
        """Stop processing."""
        self._running = False
        if self._processor_task:
            self._processor_task.cancel()
            await asyncio.gather(self._processor_task, return_exceptions=True)

    async def _process_loop(self) -> None:
        """Main processing loop."""
        while self._running:
            try:
                event = await asyncio.wait_for(self._buffer.get(), timeout=0.1)
            except asyncio.TimeoutError:
                continue

            # Process with all processors concurrently
            async with create_task_scope() as scope:
                for processor in self._processors:
                    scope.create_task(processor(event), name=f"process-{processor.__name__}")
                await scope.wait_all()


# ============================================================================
# Resource Pool
# ============================================================================

class ResourcePool:
    """
    Generic resource pool with lifecycle management.
    
    Can be used for database connections, HTTP clients, etc.
    """

    def __init__(
        self,
        factory: Callable[[], Awaitable[Any]],
        max_size: int = 10,
        min_size: int = 2,
        idle_timeout: float = 300.0,
    ):
        self._factory = factory
        self._max_size = max_size
        self._min_size = min_size
        self._idle_timeout = idle_timeout
        self._pool: deque[tuple[Any, float]] = deque()  # (resource, last_used)
        self._in_use: set[Any] = set()
        self._lock = asyncio.Lock()
        self._maintenance_task: asyncio.Task | None = None

    async def acquire(self) -> Any:
        """Acquire a resource from the pool."""
        async with self._lock:
            # Try to get from pool
            while self._pool:
                resource, last_used = self._pool.popleft()
                if time.time() - last_used < self._idle_timeout:
                    self._in_use.add(resource)
                    return resource
                # Resource expired, discard

            # Create new if under limit
            if len(self._in_use) < self._max_size:
                resource = await self._factory()
                self._in_use.add(resource)
                return resource

        # Wait for release
        while True:
            await asyncio.sleep(0.1)
            async with self._lock:
                if self._pool:
                    resource, last_used = self._pool.popleft()
                    if time.time() - last_used < self._idle_timeout:
                        self._in_use.add(resource)
                        return resource

    async def release(self, resource: Any) -> None:
        """Release resource back to pool."""
        async with self._lock:
            if resource in self._in_use:
                self._in_use.remove(resource)
                self._pool.append((resource, time.time()))

    async def start(self) -> None:
        """Pre-warm pool."""
        for _ in range(self._min_size):
            resource = await self._factory()
            self._pool.append((resource, time.time()))

    async def stop(self) -> None:
        """Close all resources."""
        async with self._lock:
            for resource, _ in self._pool:
                if hasattr(resource, 'close'):
                    await resource.close()
                elif hasattr(resource, 'aclose'):
                    await resource.aclose()
            self._pool.clear()
            self._in_use.clear()


# ============================================================================
# Circuit Breaker
# ============================================================================

class CircuitBreaker:
    """Circuit breaker for fault tolerance."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        expected_exception: type[Exception] = Exception,
    ):
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._expected_exception = expected_exception
        self._failures = 0
        self._last_failure_time: float | None = None
        self._state = "closed"  # closed, open, half-open
        self._lock = asyncio.Lock()

    @property
    def state(self) -> str:
        return self._state

    async def call(self, func: Callable[..., Awaitable[T]], *args: Any, **kwargs: Any) -> T:
        """Execute function with circuit breaker."""
        async with self._lock:
            if self._state == "open":
                if time.time() - self._last_failure_time > self._recovery_timeout:
                    self._state = "half-open"
                else:
                    raise Exception("Circuit breaker is open")

        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                self._failures = 0
                self._state = "closed"
            return result
        except self._expected_exception as e:
            async with self._lock:
                self._failures += 1
                self._last_failure_time = time.time()
                if self._failures >= self._failure_threshold:
                    self._state = "open"
            raise


# ============================================================================
# Example Usage
# ============================================================================

async def example_job_handlers(job: Job) -> dict:
    """Example job handler."""
    await asyncio.sleep(random.uniform(0.1, 0.5))
    return {"processed": True, "job_id": job.id}


async def example_event_processor(event: Event) -> None:
    """Example event processor."""
    await asyncio.sleep(0.01)
    increment("events_processed", tags={"type": event.type})


async def example_resource_factory() -> dict:
    """Example resource factory."""
    await asyncio.sleep(0.01)
    return {"id": str(uuid.uuid4()), "connected": True}


async def flaky_service() -> str:
    """Simulate a flaky service."""
    if random.random() < 0.3:
        raise ConnectionError("Service unavailable")
    return "success"


async def main():
    """Demonstrate advanced patterns."""
    loop = get_event_loop()
    tracer = get_tracer("advanced-patterns")

    print("=== Aevum Advanced Patterns Demo ===\n")

    # 1. Job Queue
    print("1. Job Queue System")
    job_queue = JobQueue(max_workers=4)
    job_queue.register_handler("process_data", example_job_handlers)
    job_queue.register_handler("send_email", example_job_handlers)

    await job_queue.start()

    # Enqueue jobs
    for i in range(10):
        job = Job(name="process_data", payload={"index": i}, priority=i)
        await job_queue.enqueue(job)

    # Wait a bit for processing
    await asyncio.sleep(1)

    await job_queue.stop()
    print("   Job queue demo complete\n")

    # 2. Event Stream
    print("2. Event Stream Processing")
    event_stream = EventStream(buffer_size=1000)
    event_stream.add_processor(example_event_processor)

    await event_stream.start()

    # Publish events
    for i in range(20):
        event = Event(type="user_action", data={"user_id": i, "action": "click"})
        await event_stream.publish(event)

    await asyncio.sleep(0.5)
    await event_stream.stop()
    print("   Event stream demo complete\n")

    # 3. Resource Pool
    print("3. Resource Pool")
    pool = ResourcePool(example_resource_factory, max_size=5, min_size=2)
    await pool.start()

    # Acquire and use resources
    async with create_task_scope() as scope:
        for _ in range(10):
            scope.create_task(use_resource(pool))

    await pool.stop()
    print("   Resource pool demo complete\n")

    # 4. Circuit Breaker
    print("4. Circuit Breaker")
    breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=1.0)

    for i in range(10):
        try:
            result = await breaker.call(flaky_service)
            print(f"   Call {i}: {result} (state: {breaker.state})")
        except Exception as e:
            print(f"   Call {i}: Failed - {e} (state: {breaker.state})")
        await asyncio.sleep(0.1)

    print("   Circuit breaker demo complete\n")

    # 5. Supervised Task Group
    print("5. Supervised Task Group")
    async with supervised_task_group(restart_on_failure=True, max_restarts=2, restart_delay=0.1) as group:
        group.create_task(reliable_task(), name="reliable")
        group.create_task(flaky_task(), name="flaky")
        await asyncio.sleep(0.5)

    print("   Supervised task group demo complete\n")

    # 6. Structured Concurrency with Cancellation
    print("6. Structured Concurrency")
    async with create_task_scope(timeout=1.0) as scope:
        scope.create_task(long_running_task(0.5), name="fast")
        scope.create_task(long_running_task(2.0), name="slow")
        results = await scope.wait_all()

    print(f"   Results: {results}\n")

    # Print metrics
    print("=== Metrics ===")
    metrics = get_metrics("advanced-patterns").get_all_metrics()
    for name, data in metrics.items():
        print(f"  {name}: {data}")


async def use_resource(pool: ResourcePool) -> None:
    """Use a resource from pool."""
    resource = await pool.acquire()
    try:
        await asyncio.sleep(0.05)
    finally:
        await pool.release(resource)


async def reliable_task() -> None:
    """A reliable task."""
    await asyncio.sleep(0.1)


async def flaky_task() -> None:
    """A task that fails occasionally."""
    if random.random() < 0.5:
        raise ValueError("Random failure")
    await asyncio.sleep(0.1)


async def long_running_task(duration: float) -> str:
    """A long-running task."""
    await asyncio.sleep(duration)
    return f"completed in {duration}s"


if __name__ == "__main__":
    config = LoopConfig(
        debug=True,
        enable_metrics=True,
        enable_tracing=True,
    )
    with LoopContext(config=config) as loop:
        loop.run_until_complete(main())