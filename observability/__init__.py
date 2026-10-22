"""
Observability: Metrics, Tracing, Health Checks

Provides comprehensive observability for async applications:
- Metrics collection and export
- Distributed tracing
- Health checks
- Structured logging integration
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import logging
import os
import time
import uuid
from collections import defaultdict, deque
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, TypeVar

from ..core.exceptions import HealthCheckError, MetricsError, TracingError
from ..core.loop import DebugEventLoop, get_event_loop

T = TypeVar("T")

logger = logging.getLogger(__name__)

# Context variables for tracing
_current_span: contextvars.ContextVar["Span | None"] = contextvars.ContextVar(
    "aevum_current_span", default=None
)
_trace_context: contextvars.ContextVar[dict[str, str]] = contextvars.ContextVar(
    "aevum_trace_context", default={}
)


@dataclass(slots=True)
class Span:
    """Distributed tracing span."""

    name: str
    trace_id: str
    span_id: str
    parent_span_id: str | None
    start_time: float
    end_time: float | None = None
    tags: dict[str, str] = field(default_factory=dict)
    logs: list[dict] = field(default_factory=list)
    status: str = "ok"

    @property
    def duration(self) -> float | None:
        if self.end_time:
            return self.end_time - self.start_time
        return None

    def finish(self, status: str = "ok") -> None:
        """Finish the span."""
        self.end_time = time.time()
        self.status = status

    def set_tag(self, key: str, value: str) -> None:
        """Set a tag on the span."""
        self.tags[key] = value

    def log(self, event: str, **fields: Any) -> None:
        """Add a log event to the span."""
        self.logs.append({
            "timestamp": time.time(),
            "event": event,
            **fields,
        })

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration": self.duration,
            "tags": self.tags,
            "logs": self.logs,
            "status": self.status,
        }


class Tracer:
    """
    Distributed tracer for async operations.

    Features:
    - Automatic context propagation
    - Span hierarchy
    - Sampling
    - Export to multiple backends
    """

    def __init__(
        self,
        service_name: str = "aevum",
        sampler: Callable[[str], bool] | None = None,
        max_spans: int = 10000,
    ):
        self._service_name = service_name
        self._sampler = sampler or (lambda _: True)
        self._spans: deque[Span] = deque(maxlen=max_spans)
        self._exporters: list[Callable[[list[Span]], Any]] = []
        self._span_counter = 0

    def start_span(
        self,
        name: str,
        *,
        parent: Span | None = None,
        tags: dict[str, str] | None = None,
    ) -> Span:
        """Start a new span."""
        if not self._sampler(name):
            return _NoopSpan()

        self._span_counter += 1
        trace_id = parent.trace_id if parent else str(uuid.uuid4())[:8]
        span_id = f"{trace_id}-{self._span_counter:04d}"
        parent_span_id = parent.span_id if parent else None

        span = Span(
            name=name,
            trace_id=trace_id,
            span_id=span_id,
            parent_span_id=parent_span_id,
            start_time=time.time(),
            tags=tags or {},
        )

        # Add service name tag
        span.set_tag("service", self._service_name)

        return span

    @asynccontextmanager
    async def span(
        self,
        name: str,
        *,
        parent: Span | None = None,
        tags: dict[str, str] | None = None,
    ) -> AsyncIterator[Span]:
        """Context manager for span."""
        span = self.start_span(name, parent=parent, tags=tags)
        token = _current_span.set(span)
        try:
            yield span
        except Exception as e:
            span.set_tag("error", "true")
            span.log("exception", error=str(e), type=type(e).__name__)
            span.finish("error")
            raise
        else:
            span.finish("ok")
        finally:
            _current_span.reset(token)
            self._spans.append(span)
            for exporter in self._exporters:
                try:
                    exporter([span])
                except Exception:
                    pass

    def add_exporter(self, exporter: Callable[[list[Span]], Any]) -> None:
        """Add a span exporter."""
        self._exporters.append(exporter)

    def get_spans(self, trace_id: str | None = None) -> list[Span]:
        """Get recorded spans."""
        if trace_id:
            return [s for s in self._spans if s.trace_id == trace_id]
        return list(self._spans)

    def clear(self) -> None:
        """Clear recorded spans."""
        self._spans.clear()


class _NoopSpan:
    """No-op span for when sampling is disabled."""

    def __init__(self):
        self.name = ""
        self.trace_id = ""
        self.span_id = ""
        self.parent_span_id = None
        self.start_time = 0
        self.end_time = None

    def finish(self, status: str = "ok") -> None:
        pass

    def set_tag(self, key: str, value: str) -> None:
        pass

    def log(self, event: str, **fields: Any) -> None:
        pass

    def to_dict(self) -> dict:
        return {}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


def get_current_span() -> Span | None:
    """Get the current active span."""
    return _current_span.get(None)


def set_span_tag(key: str, value: str) -> None:
    """Set a tag on the current span."""
    span = get_current_span()
    if span:
        span.set_tag(key, value)


def add_span_log(event: str, **fields: Any) -> None:
    """Add a log to the current span."""
    span = get_current_span()
    if span:
        span.log(event, **fields)


def trace(
    name: str | None = None,
    *,
    tags: dict[str, str] | None = None,
) -> Callable:
    """Decorator to trace a function."""
    def decorator(func: Callable) -> Callable:
        span_name = name or f"{func.__module__}.{func.__qualname__}"

        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            tracer = get_tracer()
            async with tracer.span(span_name, tags=tags):
                return await func(*args, **kwargs)

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            tracer = get_tracer()
            with tracer.span(span_name, tags=tags) as span:
                return func(*args, **kwargs)

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper

    return decorator


_tracer: Tracer | None = None


def get_tracer(service_name: str = "aevum") -> Tracer:
    """Get the global tracer."""
    global _tracer
    if _tracer is None:
        _tracer = Tracer(service_name)
    return _tracer


def set_tracer(tracer: Tracer) -> None:
    """Set the global tracer."""
    global _tracer
    _tracer = tracer


# ========================================================================
# Metrics
# ========================================================================

@dataclass(slots=True)
class MetricPoint:
    """Single metric data point."""

    name: str
    value: float
    timestamp: float
    tags: dict[str, str] = field(default_factory=dict)
    metric_type: str = "gauge"  # gauge, counter, histogram, summary


class MetricsCollector:
    """
    Collects and aggregates metrics.

    Features:
    - Multiple metric types
    - Tagging/dimensions
    - Aggregation
    - Export to Prometheus, StatsD, etc.
    """

    def __init__(self, prefix: str = "aevum"):
        self._prefix = prefix
        self._metrics: dict[str, deque[MetricPoint]] = defaultdict(lambda: deque(maxlen=10000))
        self._counters: dict[str, float] = defaultdict(float)
        self._gauges: dict[str, float] = {}
        self._histograms: dict[str, list[float]] = defaultdict(list)
        self._lock = asyncio.Lock()

    def _full_name(self, name: str) -> str:
        return f"{self._prefix}.{name}"

    def increment(self, name: str, value: float = 1.0, tags: dict[str, str] | None = None) -> None:
        """Increment a counter."""
        full_name = self._full_name(name)
        self._counters[full_name] += value
        self._record(full_name, self._counters[full_name], tags, "counter")

    def gauge(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        """Set a gauge value."""
        full_name = self._full_name(name)
        self._gauges[full_name] = value
        self._record(full_name, value, tags, "gauge")

    def histogram(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        """Record a histogram value."""
        full_name = self._full_name(name)
        self._histograms[full_name].append(value)
        # Keep last 1000 values
        if len(self._histograms[full_name]) > 1000:
            self._histograms[full_name] = self._histograms[full_name][-1000:]
        self._record(full_name, value, tags, "histogram")

    def timing(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        """Record a timing (in seconds)."""
        self.histogram(f"{name}.duration", value, tags)

    def _record(
        self,
        name: str,
        value: float,
        tags: dict[str, str] | None,
        metric_type: str,
    ) -> None:
        point = MetricPoint(
            name=name,
            value=value,
            timestamp=time.time(),
            tags=tags or {},
            metric_type=metric_type,
        )
        self._metrics[name].append(point)

    def get_counter(self, name: str) -> float:
        """Get counter value."""
        return self._counters.get(self._full_name(name), 0.0)

    def get_gauge(self, name: str) -> float | None:
        """Get gauge value."""
        return self._gauges.get(self._full_name(name))

    def get_histogram_stats(self, name: str) -> dict[str, float]:
        """Get histogram statistics."""
        full_name = self._full_name(name)
        values = self._histograms.get(full_name, [])
        if not values:
            return {}
        sorted_values = sorted(values)
        n = len(sorted_values)
        return {
            "count": n,
            "min": sorted_values[0],
            "max": sorted_values[-1],
            "mean": sum(sorted_values) / n,
            "p50": sorted_values[n // 2],
            "p90": sorted_values[int(n * 0.9)],
            "p95": sorted_values[int(n * 0.95)],
            "p99": sorted_values[int(n * 0.99)],
        }

    def get_all_metrics(self) -> dict[str, Any]:
        """Get all metrics."""
        result = {}
        for name, value in self._counters.items():
            result[name] = {"type": "counter", "value": value}
        for name, value in self._gauges.items():
            result[name] = {"type": "gauge", "value": value}
        for name in self._histograms:
            result[name] = {"type": "histogram", "stats": self.get_histogram_stats(name)}
        return result

    def export_prometheus(self) -> str:
        """Export metrics in Prometheus format."""
        lines = []
        for name, value in self._counters.items():
            lines.append(f"# TYPE {name} counter")
            lines.append(f"{name} {value}")
        for name, value in self._gauges.items():
            lines.append(f"# TYPE {name} gauge")
            lines.append(f"{name} {value}")
        for name in self._histograms:
            stats = self.get_histogram_stats(name)
            lines.append(f"# TYPE {name} summary")
            for stat, value in stats.items():
                lines.append(f"{name}_{stat} {value}")
        return "\n".join(lines)


_metrics_collector: MetricsCollector | None = None


def get_metrics(prefix: str = "aevum") -> MetricsCollector:
    """Get the global metrics collector."""
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector(prefix)
    return _metrics_collector


def increment(name: str, value: float = 1.0, tags: dict[str, str] | None = None) -> None:
    """Increment a counter."""
    get_metrics().increment(name, value, tags)


def gauge(name: str, value: float, tags: dict[str, str] | None = None) -> None:
    """Set a gauge."""
    get_metrics().gauge(name, value, tags)


def histogram(name: str, value: float, tags: dict[str, str] | None = None) -> None:
    """Record histogram value."""
    get_metrics().histogram(name, value, tags)


def timing(name: str, value: float, tags: dict[str, str] | None = None) -> None:
    """Record timing."""
    get_metrics().timing(name, value, tags)


@asynccontextmanager
async def measure_time(name: str, tags: dict[str, str] | None = None) -> AsyncIterator[None]:
    """Context manager to measure execution time."""
    start = time.perf_counter()
    try:
        yield
    finally:
        timing(name, time.perf_counter() - start, tags)


# ========================================================================
# Health Checks
# ========================================================================

@dataclass(slots=True)
class HealthCheckResult:
    """Result of a health check."""

    name: str
    healthy: bool
    message: str = ""
    details: dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    duration: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "healthy": self.healthy,
            "message": self.message,
            "details": self.details,
            "timestamp": self.timestamp,
            "duration": self.duration,
        }


class HealthCheck:
    """Base class for health checks."""

    def __init__(self, name: str, timeout: float = 5.0):
        self.name = name
        self.timeout = timeout

    async def check(self) -> HealthCheckResult:
        """Perform the health check."""
        start = time.perf_counter()
        try:
            result = await asyncio.wait_for(self._check(), timeout=self.timeout)
            duration = time.perf_counter() - start
            if isinstance(result, HealthCheckResult):
                result.duration = duration
                return result
            return HealthCheckResult(
                name=self.name,
                healthy=bool(result),
                duration=duration,
            )
        except asyncio.TimeoutError:
            return HealthCheckResult(
                name=self.name,
                healthy=False,
                message=f"Health check timed out after {self.timeout}s",
                duration=time.perf_counter() - start,
            )
        except Exception as e:
            return HealthCheckResult(
                name=self.name,
                healthy=False,
                message=str(e),
                duration=time.perf_counter() - start,
            )

    async def _check(self) -> bool | HealthCheckResult:
        raise NotImplementedError


class HealthCheckRegistry:
    """Registry for health checks."""

    def __init__(self):
        self._checks: dict[str, HealthCheck] = {}
        self._results: dict[str, HealthCheckResult] = {}

    def register(self, check: HealthCheck) -> None:
        """Register a health check."""
        if check.name in self._checks:
            raise ValueError(f"Health check '{check.name}' already registered")
        self._checks[check.name] = check

    def unregister(self, name: str) -> bool:
        """Unregister a health check."""
        return self._checks.pop(name, None) is not None

    async def run_check(self, name: str) -> HealthCheckResult:
        """Run a specific health check."""
        check = self._checks.get(name)
        if not check:
            raise HealthCheckError(f"Health check '{name}' not found", check_name=name)
        result = await check.check()
        self._results[name] = result
        return result

    async def run_all(self) -> dict[str, HealthCheckResult]:
        """Run all health checks."""
        results = {}
        for name, check in self._checks.items():
            results[name] = await check.check()
        self._results = results
        return results

    def get_result(self, name: str) -> HealthCheckResult | None:
        """Get last result for a check."""
        return self._results.get(name)

    def get_all_results(self) -> dict[str, HealthCheckResult]:
        """Get all last results."""
        return self._results.copy()

    def is_healthy(self) -> bool:
        """Check if all registered checks are healthy."""
        return all(r.healthy for r in self._results.values())


# Built-in health checks
class LoopHealthCheck(HealthCheck):
    """Check event loop health."""

    def __init__(self, loop: DebugEventLoop | None = None, max_pending_tasks: int = 1000):
        super().__init__("event_loop")
        self._loop = loop or get_event_loop()
        self._max_pending_tasks = max_pending_tasks

    async def _check(self) -> HealthCheckResult:
        pending = len(self._loop.get_pending_tasks())
        healthy = pending < self._max_pending_tasks
        return HealthCheckResult(
            name=self.name,
            healthy=healthy,
            message=f"Pending tasks: {pending}" if not healthy else "OK",
            details={"pending_tasks": pending, "max_pending": self._max_pending_tasks},
        )


class MemoryHealthCheck(HealthCheck):
    """Check memory usage."""

    def __init__(self, max_memory_mb: float = 1024):
        super().__init__("memory")
        self._max_memory_mb = max_memory_mb

    async def _check(self) -> HealthCheckResult:
        try:
            import psutil
            process = psutil.Process(os.getpid())
            mem_mb = process.memory_info().rss / 1024 / 1024
            healthy = mem_mb < self._max_memory_mb
            return HealthCheckResult(
                name=self.name,
                healthy=healthy,
                message=f"Memory: {mem_mb:.1f}MB" if not healthy else "OK",
                details={"memory_mb": mem_mb, "max_memory_mb": self._max_memory_mb},
            )
        except ImportError:
            return HealthCheckResult(
                name=self.name,
                healthy=True,
                message="psutil not available",
            )


class DiskHealthCheck(HealthCheck):
    """Check disk space."""

    def __init__(self, path: str = "/", min_free_gb: float = 1.0):
        super().__init__("disk")
        self._path = path
        self._min_free_gb = min_free_gb

    async def _check(self) -> HealthCheckResult:
        try:
            import psutil
            usage = psutil.disk_usage(self._path)
            free_gb = usage.free / 1024 / 1024 / 1024
            healthy = free_gb >= self._min_free_gb
            return HealthCheckResult(
                name=self.name,
                healthy=healthy,
                message=f"Free disk: {free_gb:.1f}GB" if not healthy else "OK",
                details={"free_gb": free_gb, "min_free_gb": self._min_free_gb, "path": self._path},
            )
        except ImportError:
            return HealthCheckResult(
                name=self.name,
                healthy=True,
                message="psutil not available",
            )


_health_registry: HealthCheckRegistry | None = None


def get_health_registry() -> HealthCheckRegistry:
    """Get the global health check registry."""
    global _health_registry
    if _health_registry is None:
        _health_registry = HealthCheckRegistry()
        # Register default checks
        _health_registry.register(LoopHealthCheck())
        _health_registry.register(MemoryHealthCheck())
        _health_registry.register(DiskHealthCheck())
    return _health_registry


async def run_health_checks() -> dict[str, HealthCheckResult]:
    """Run all health checks."""
    return await get_health_registry().run_all()


def is_healthy() -> bool:
    """Check if all health checks pass."""
    return get_health_registry().is_healthy()


# ========================================================================
# Structured Logging Integration
# ========================================================================

class StructuredLogger:
    """Structured logger with trace context."""

    def __init__(self, name: str):
        self._logger = logging.getLogger(name)

    def _log(self, level: int, message: str, **fields: Any) -> None:
        span = get_current_span()
        extra = {}
        if span:
            extra["trace_id"] = span.trace_id
            extra["span_id"] = span.span_id
        extra.update(fields)
        self._logger.log(level, message, extra=extra)

    def debug(self, message: str, **fields: Any) -> None:
        self._log(logging.DEBUG, message, **fields)

    def info(self, message: str, **fields: Any) -> None:
        self._log(logging.INFO, message, **fields)

    def warning(self, message: str, **fields: Any) -> None:
        self._log(logging.WARNING, message, **fields)

    def error(self, message: str, **fields: Any) -> None:
        self._log(logging.ERROR, message, **fields)

    def exception(self, message: str, **fields: Any) -> None:
        self._log(logging.ERROR, message, exc_info=True, **fields)


def get_logger(name: str) -> StructuredLogger:
    """Get a structured logger."""
    return StructuredLogger(name)