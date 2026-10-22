"""
Tests for Aevum Tasks
"""

import asyncio
import pytest

from aevum import (
    create_task_scope,
    supervised_task_group,
    TaskScope,
    SupervisedTaskGroup,
    CancellationScope,
    gather_with_concurrency,
    wait_for_first,
    wait_for_all,
    set_task_local,
    get_task_local,
    clear_task_local,
    TaskMonitor,
    enable_task_monitoring,
)


class TestTaskScope:
    @pytest.mark.asyncio
    async def test_basic_scope(self):
        async with create_task_scope() as scope:
            task = scope.create_task(asyncio.sleep(0, result="done"))
            results = await scope.wait_all()
            assert results == ["done"]

    @pytest.mark.asyncio
    async def test_scope_cancellation(self):
        cancelled = False

        async def long_task():
            nonlocal cancelled
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                cancelled = True
                raise

        async with create_task_scope() as scope:
            scope.create_task(long_task())
            # Scope exits, should cancel

        assert cancelled

    @pytest.mark.asyncio
    async def test_scope_with_exception(self):
        async def failing_task():
            raise ValueError("test error")

        with pytest.raises(ValueError, match="test error"):
            async with create_task_scope() as scope:
                scope.create_task(failing_task())
                await scope.wait_all()

    @pytest.mark.asyncio
    async def test_scope_timeout(self):
        async with create_task_scope(timeout=0.01) as scope:
            scope.create_task(asyncio.sleep(1))
            with pytest.raises(asyncio.TimeoutError):
                await scope.wait_all()

    @pytest.mark.asyncio
    async def test_scope_wait_any(self):
        async def fast():
            await asyncio.sleep(0.01)
            return "fast"

        async def slow():
            await asyncio.sleep(1)
            return "slow"

        async with create_task_scope() as scope:
            scope.create_task(fast())
            scope.create_task(slow())
            task, result = await scope.wait_any()
            assert result == "fast"

    @pytest.mark.asyncio
    async def test_nested_scopes(self):
        async with create_task_scope() as outer:
            outer.create_task(asyncio.sleep(0, result="outer"))
            async with create_task_scope() as inner:
                inner.create_task(asyncio.sleep(0, result="inner"))
                results = await inner.wait_all()
                assert results == ["inner"]
            results = await outer.wait_all()
            assert "outer" in str(results)


class TestSupervisedTaskGroup:
    @pytest.mark.asyncio
    async def test_basic_supervision(self):
        async with supervised_task_group() as group:
            task = group.create_task(asyncio.sleep(0, result="supervised"))
            results = await group.wait_all()
            assert results == {"task-0": "supervised"}

    @pytest.mark.asyncio
    async def test_restart_on_failure(self):
        attempts = []

        async def flaky_task():
            attempts.append(1)
            if len(attempts) < 3:
                raise ValueError("fail")
            return "success"

        async with supervised_task_group(restart_on_failure=True, max_restarts=3, restart_delay=0.01) as group:
            group.create_task(flaky_task(), name="flaky")
            results = await group.wait_all()
            assert results["flaky"] == "success"
            assert len(attempts) == 3

    @pytest.mark.asyncio
    async def test_max_restarts_exceeded(self):
        async def always_fails():
            raise ValueError("always fails")

        async with supervised_task_group(restart_on_failure=True, max_restarts=2, restart_delay=0.01) as group:
            group.create_task(always_fails(), name="fail")
            results = await group.wait_all()
            assert isinstance(results["fail"], ValueError)


class TestCancellationScope:
    @pytest.mark.asyncio
    async def test_cancellation_scope(self):
        async with CancellationScope() as scope:
            assert not scope.cancelled

        # After exit, should be cancelled
        # (Actually scope only cancels on explicit cancel())

    @pytest.mark.asyncio
    async def test_explicit_cancel(self):
        scope = CancellationScope()
        scope.__enter__()
        scope.cancel()
        assert scope.cancelled
        scope.__exit__(None, None, None)

    @pytest.mark.asyncio
    async def test_nested_cancellation(self):
        parent = CancellationScope()
        parent.__enter__()

        child = CancellationScope()
        child.__enter__()

        parent.cancel()
        assert parent.cancelled
        assert child.cancelled  # Child inherits parent cancellation

        child.__exit__(None, None, None)
        parent.__exit__(None, None, None)


class TestGatherWithConcurrency:
    @pytest.mark.asyncio
    async def test_concurrency_limit(self):
        running = 0
        max_running = 0

        async def work():
            nonlocal running, max_running
            running += 1
            max_running = max(max_running, running)
            await asyncio.sleep(0.01)
            running -= 1
            return running

        results = await gather_with_concurrency(
            *(work() for _ in range(10)),
            max_concurrent=3,
        )
        assert max_running <= 3
        assert len(results) == 10

    @pytest.mark.asyncio
    async def test_preserves_order(self):
        async def delayed(i):
            await asyncio.sleep(0.01 * (10 - i))
            return i

        results = await gather_with_concurrency(
            *(delayed(i) for i in range(5)),
            max_concurrent=2,
        )
        assert results == [0, 1, 2, 3, 4]


class TestWaitForFirst:
    @pytest.mark.asyncio
    async def test_wait_for_first(self):
        async def slow():
            await asyncio.sleep(1)
            return "slow"

        async def fast():
            await asyncio.sleep(0.01)
            return "fast"

        index, result = await wait_for_first(slow(), fast())
        assert index == 1
        assert result == "fast"


class TestWaitForAll:
    @pytest.mark.asyncio
    async def test_wait_for_all(self):
        results = await wait_for_all(
            asyncio.sleep(0, result=1),
            asyncio.sleep(0, result=2),
            asyncio.sleep(0, result=3),
        )
        assert results == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_wait_for_all_with_exceptions(self):
        async def fail():
            raise ValueError("error")

        results = await wait_for_all(
            asyncio.sleep(0, result="ok"),
            fail(),
            return_exceptions=True,
        )
        assert results[0] == "ok"
        assert isinstance(results[1], ValueError)


class TestTaskLocalStorage:
    @pytest.mark.asyncio
    async def test_task_local(self):
        clear_task_local()
        set_task_local("key", "value")
        assert get_task_local("key") == "value"

    @pytest.mark.asyncio
    async def test_task_local_isolation(self):
        clear_task_local()

        async def task1():
            set_task_local("key", "task1")
            await asyncio.sleep(0.01)
            return get_task_local("key")

        async def task2():
            set_task_local("key", "task2")
            await asyncio.sleep(0.01)
            return get_task_local("key")

        results = await asyncio.gather(task1(), task2())
        assert results == ["task1", "task2"]

    @pytest.mark.asyncio
    async def test_task_local_default(self):
        clear_task_local()
        assert get_task_local("missing", "default") == "default"


class TestTaskMonitor:
    @pytest.mark.asyncio
    async def test_monitor_events(self):
        monitor = TaskMonitor()
        enable_task_monitoring(monitor)

        async def work():
            await asyncio.sleep(0.01)

        async with create_task_scope() as scope:
            scope.create_task(work())
            scope.create_task(work())
            await scope.wait_all()

        events = monitor.get_events()
        assert len(events) >= 2
        created_events = monitor.get_events("created")
        assert len(created_events) >= 2