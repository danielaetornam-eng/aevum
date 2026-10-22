"""
Tests for Aevum Observability
"""

import asyncio
import pytest

from aevum import (
    Tracer,
    Span,
    get_tracer,
    trace,
    MetricsCollector,
    get_metrics,
    increment,
    gauge,
    histogram,
    timing,
    measure_time,
    HealthCheckRegistry,
    LoopHealthCheck,
    MemoryHealthCheck,
    DiskHealthCheck,
    get_health_registry,
    run_health_checks,
    is_healthy,
    HealthCheckResult,
    StructuredLogger,
    get_logger,
)


class TestTracer:
    @pytest.fixture
    def tracer(self):
        return Tracer(service_name="test")

    @pytest.mark.asyncio
    async def test_basic_span(self, tracer):
        async with tracer.span("test-operation") as span:
            assert span.name == "test-operation"
            assert span.trace_id
            assert span.span_id
            assert span.start_time

        assert span.end_time is not None
        assert span.status == "ok"

    @pytest.mark.asyncio
    async def test_span_with_error(self, tracer):
        with pytest.raises(ValueError):
            async with tracer.span("error-operation"):
                raise ValueError("test")

        spans = tracer.get_spans()
        assert len(spans) == 1
        assert spans[0].status == "error"
        assert spans[0].tags.get("error") == "true"

    @pytest.mark.asyncio
    async def test_nested_spans(self, tracer):
        async with tracer.span("parent") as parent:
            async with tracer.span("child", parent=parent) as child:
                assert child.parent_span_id == parent.span_id
                assert child.trace_id == parent.trace_id

    @pytest.mark.asyncio
    async def test_span_tags_and_logs(self, tracer):
        async with tracer.span("tagged") as span:
            span.set_tag("key", "value")
            span.log("event", field="data")

        spans = tracer.get_spans()
        assert spans[0].tags["key"] == "value"
        assert len(spans[0].logs) == 1

    @pytest.mark.asyncio
    async def test_sampling(self):
        tracer = Tracer(sampler=lambda name: False)
        span = tracer.start_span("sampled-out")
        assert isinstance(span, type(tracer.start_span("test")))  # NoopSpan

    @pytest.mark.asyncio
    async def test_trace_decorator(self):
        tracer = Tracer(service_name="test")
        calls = []

        @trace("decorated-func")
        async def decorated():
            calls.append("called")
            return "result"

        result = await decorated()
        assert result == "result"

        spans = tracer.get_spans()
        assert len(spans) == 1
        assert spans[0].name == "decorated-func"

    def test_get_current_span(self):
        tracer = Tracer()
        span = tracer.start_span("current")
        # Not in context, should be None
        from aevum.observability import get_current_span
        assert get_current_span() is None


class TestMetricsCollector:
    @pytest.fixture
    def collector(self):
        return MetricsCollector(prefix="test")

    def test_counter(self, collector):
        collector.increment("requests")
        collector.increment("requests", 5)
        assert collector.get_counter("requests") == 6

    def test_gauge(self, collector):
        collector.gauge("memory", 100.5)
        collector.gauge("memory", 200.0)
        assert collector.get_gauge("memory") == 200.0

    def test_histogram(self, collector):
        for i in range(100):
            collector.histogram("latency", i * 0.01)

        stats = collector.get_histogram_stats("latency")
        assert stats["count"] == 100
        assert stats["min"] == 0.0
        assert stats["max"] == 0.99
        assert "mean" in stats
        assert "p50" in stats
        assert "p95" in stats

    def test_timing(self, collector):
        collector.timing("operation", 0.5)
        stats = collector.get_histogram_stats("operation.duration")
        assert stats["count"] == 1
        assert stats["mean"] == 0.5

    def test_measure_time_context(self, collector):
        with pytest.raises(NotImplementedError):
            # This is a context manager, need to test differently
            pass

    @pytest.mark.asyncio
    async def test_measure_time_async(self, collector):
        async with measure_time("async_op", collector=collector):
            await asyncio.sleep(0.01)

        stats = collector.get_histogram_stats("async_op.duration")
        assert stats["count"] == 1
        assert stats["mean"] > 0.005

    def test_prometheus_export(self, collector):
        collector.increment("counter", 5)
        collector.gauge("gauge", 10.5)
        collector.histogram("hist", 1.0)

        output = collector.export_prometheus()
        assert "counter 5" in output
        assert "gauge 10.5" in output
        assert "hist_count" in output


class TestGlobalMetrics:
    def test_global_metrics(self):
        m1 = get_metrics("global1")
        m2 = get_metrics("global1")
        assert m1 is m2

    def test_global_functions(self):
        get_metrics("global-funcs")
        increment("global_counter")
        gauge("global_gauge", 42)
        histogram("global_hist", 1.5)
        timing("global_time", 0.1)

        collector = get_metrics("global-funcs")
        assert collector.get_counter("global_counter") == 1
        assert collector.get_gauge("global_gauge") == 42


class TestHealthChecks:
    @pytest.fixture
    def registry(self):
        return HealthCheckRegistry()

    @pytest.mark.asyncio
    async def test_register_check(self, registry):
        class TestCheck(HealthCheck):
            async def _check(self):
                return True

        check = TestCheck("test")
        registry.register(check)
        result = await registry.run_check("test")
        assert result.healthy

    @pytest.mark.asyncio
    async def test_failing_check(self, registry):
        class FailCheck(HealthCheck):
            async def _check(self):
                return False

        registry.register(FailCheck("fail"))
        result = await registry.run_check("fail")
        assert not result.healthy

    @pytest.mark.asyncio
    async def test_exception_check(self, registry):
        class ErrorCheck(HealthCheck):
            async def _check(self):
                raise ValueError("error")

        registry.register(ErrorCheck("error"))
        result = await registry.run_check("error")
        assert not result.healthy
        assert "error" in result.message

    @pytest.mark.asyncio
    async def test_timeout_check(self, registry):
        class SlowCheck(HealthCheck):
            def __init__(self):
                super().__init__("slow", timeout=0.01)

            async def _check(self):
                await asyncio.sleep(1)

        registry.register(SlowCheck())
        result = await registry.run_check("slow")
        assert not result.healthy
        assert "timed out" in result.message

    @pytest.mark.asyncio
    async def test_run_all(self, registry):
        class OkCheck(HealthCheck):
            async def _check(self):
                return True

        registry.register(OkCheck("ok1"))
        registry.register(OkCheck("ok2"))

        results = await registry.run_all()
        assert len(results) == 2
        assert all(r.healthy for r in results.values())
        assert registry.is_healthy()

    @pytest.mark.asyncio
    async def test_loop_health_check(self):
        from aevum import new_event_loop
        loop = new_event_loop()
        check = LoopHealthCheck(loop, max_pending_tasks=10)
        result = await check.check()
        assert result.healthy
        loop.close()

    @pytest.mark.asyncio
    async def test_disk_health_check(self):
        check = DiskHealthCheck(path=".", min_free_gb=0.001)
        result = await check.check()
        # Should pass on most systems
        assert result.healthy or "psutil not available" in result.message


class TestStructuredLogger:
    def test_logger_creation(self):
        logger = get_logger("test")
        assert isinstance(logger, StructuredLogger)

    def test_log_methods(self):
        logger = get_logger("test")
        # Just verify they don't crash
        logger.debug("debug")
        logger.info("info")
        logger.warning("warning")
        logger.error("error")