"""
Tests for Aevum Executors
"""

import asyncio
import pytest

from aevum import (
    ManagedThreadPoolExecutor,
    ProcessPoolExecutor,
    ExecutorManager,
    create_thread_pool,
    create_process_pool,
    run_in_executor,
    get_executor_manager,
)


class TestManagedThreadPoolExecutor:
    @pytest.fixture
    def executor(self):
        exec = ManagedThreadPoolExecutor(max_workers=4, name="test-pool")
        yield exec
        exec.shutdown(wait=True)

    def test_executor_creation(self, executor):
        assert executor.name == "test-pool"
        assert executor.max_workers == 4
        assert not executor.is_shutdown

    def test_submit_sync(self, executor):
        def add(a, b):
            return a + b

        future = executor.submit(add, 2, 3)
        result = future.result()
        assert result == 5
        assert executor.metrics.submitted == 1
        assert executor.metrics.completed == 1

    @pytest.mark.asyncio
    async def test_run_in_executor(self, executor):
        def blocking_work(x):
            import time
            time.sleep(0.01)
            return x * 2

        result = await executor.run_in_executor(blocking_work, 5)
        assert result == 10

    def test_map(self, executor):
        def square(x):
            return x * x

        results = executor.map(square, range(5))
        assert results == [0, 1, 4, 9, 16]

    @pytest.mark.asyncio
    async def test_map_async(self, executor):
        async def async_square(x):
            await asyncio.sleep(0.001)
            return x * x

        results = await executor.map_async(async_square, range(5))
        assert results == [0, 1, 4, 9, 16]

    def test_shutdown(self, executor):
        executor.shutdown(wait=True)
        assert executor.is_shutdown

        with pytest.raises(Exception):  # ExecutorShutdownError
            executor.submit(lambda: None)

    def test_context_manager(self):
        with ManagedThreadPoolExecutor(max_workers=2) as executor:
            future = executor.submit(lambda: 42)
            assert future.result() == 42
        # Should be shut down after context


class TestProcessPoolExecutor:
    @pytest.fixture
    def executor(self):
        exec = ProcessPoolExecutor(max_workers=2, name="test-process-pool")
        yield exec
        exec.shutdown(wait=True)

    @pytest.mark.asyncio
    async def test_process_pool(self, executor):
        def cpu_work(n):
            return sum(i * i for i in range(n))

        result = await executor.run_in_executor(cpu_work, 1000)
        assert result == sum(i * i for i in range(1000))


class TestExecutorManager:
    @pytest.fixture
    def manager(self):
        mgr = ExecutorManager()
        yield mgr
        mgr.shutdown_all(wait=True)

    def test_create_thread_pool(self, manager):
        pool = manager.create_thread_pool("pool1", max_workers=2)
        assert pool.name == "pool1"
        assert manager.get_executor("pool1") is pool

    def test_create_process_pool(self, manager):
        pool = manager.create_process_pool("proc1", max_workers=2)
        assert pool.name == "proc1"
        assert manager.get_executor("proc1") is pool

    def test_default_executor(self, manager):
        pool1 = manager.create_thread_pool("pool1")
        pool2 = manager.create_thread_pool("pool2")

        assert manager.get_default() is pool1

        manager.set_default("pool2")
        assert manager.get_default() is pool2

    @pytest.mark.asyncio
    async def test_run_in_executor(self, manager):
        manager.create_thread_pool("default")

        def work(x):
            return x + 1

        result = await manager.run_in_executor(work, 5)
        assert result == 6

    def test_get_all_metrics(self, manager):
        pool = manager.create_thread_pool("metrics-pool")
        pool.submit(lambda: None).result()

        metrics = manager.get_all_metrics()
        assert "metrics-pool" in metrics
        assert metrics["metrics-pool"].submitted == 1

    def test_shutdown_all(self, manager):
        manager.create_thread_pool("p1")
        manager.create_process_pool("p2")
        manager.shutdown_all(wait=True)
        assert len(manager._executors) == 0


class TestGlobalExecutorManager:
    def test_global_manager(self):
        mgr1 = get_executor_manager()
        mgr2 = get_executor_manager()
        assert mgr1 is mgr2

    def test_create_thread_pool_global(self):
        pool = create_thread_pool("global-pool", max_workers=2)
        assert pool.name == "global-pool"
        pool.shutdown(wait=True)

    @pytest.mark.asyncio
    async def test_run_in_executor_global(self):
        create_thread_pool("global-default")

        def work():
            return "global"

        result = await run_in_executor(work)
        assert result == "global"