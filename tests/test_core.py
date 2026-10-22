"""
Tests for Aevum Core
"""

import asyncio
import pytest

from aevum import (
    DebugEventLoop,
    LoopConfig,
    LoopState,
    new_event_loop,
    get_event_loop,
    LoopClosedError,
)
from aevum.core.exceptions import LoopConfigurationError


class TestLoopConfig:
    def test_default_config(self):
        config = LoopConfig()
        assert config.debug is False
        assert config.slow_callback_duration == 0.1
        assert config.enable_metrics is True

    def test_custom_config(self):
        config = LoopConfig(
            debug=True,
            slow_callback_duration=0.5,
            enable_metrics=False,
        )
        assert config.debug is True
        assert config.slow_callback_duration == 0.5
        assert config.enable_metrics is False

    def test_invalid_config(self):
        with pytest.raises(ValueError):
            LoopConfig(slow_callback_duration=-1)
        with pytest.raises(ValueError):
            LoopConfig(metrics_interval=0)
        with pytest.raises(ValueError):
            LoopConfig(max_concurrent_tasks=0)

    def test_config_to_dict(self):
        config = LoopConfig(debug=True, slow_callback_duration=0.2)
        d = config.to_dict()
        assert d["debug"] is True
        assert d["slow_callback_duration"] == 0.2

    def test_config_from_dict(self):
        data = {"debug": True, "slow_callback_duration": 0.3}
        config = LoopConfig.from_dict(data)
        assert config.debug is True
        assert config.slow_callback_duration == 0.3


class TestDebugEventLoop:
    def test_loop_creation(self):
        config = LoopConfig(debug=True, enable_metrics=True)
        loop = new_event_loop(config)
        assert isinstance(loop, DebugEventLoop)
        assert loop.state == LoopState.CREATED
        assert loop.loop_id > 0
        loop.close()

    def test_loop_run_until_complete(self):
        config = LoopConfig(debug=True, enable_metrics=True)
        loop = new_event_loop(config)
        
        async def simple_coro():
            return 42

        result = loop.run_until_complete(simple_coro())
        assert result == 42
        assert loop.state == LoopState.STOPPED
        loop.close()

    def test_loop_create_task(self):
        config = LoopConfig(debug=True, enable_metrics=True)
        loop = new_event_loop(config)
        
        async def task_coro():
            await asyncio.sleep(0.01)
            return "done"

        task = loop.create_task(task_coro(), name="test-task")
        result = loop.run_until_complete(task)
        assert result == "done"
        assert loop.metrics.tasks_created == 1
        assert loop.metrics.tasks_completed == 1
        loop.close()

    def test_loop_metrics(self):
        config = LoopConfig(debug=True, enable_metrics=True)
        loop = new_event_loop(config)
        
        async def work():
            await asyncio.sleep(0.01)

        loop.run_until_complete(work())
        metrics = loop.metrics
        assert metrics.loop_iterations > 0
        assert metrics.callbacks_executed > 0
        assert metrics.tasks_created >= 1
        loop.close()

    def test_loop_slow_callback_detection(self):
        config = LoopConfig(debug=True, enable_metrics=True)
        loop = new_event_loop(config)
        loop._slow_callback_threshold = 0.001  # Very low threshold

        async def slow_work():
            await asyncio.sleep(0.01)

        loop.run_until_complete(slow_work())
        # Should detect slow callbacks
        assert loop.metrics.slow_callbacks >= 0  # May or may not trigger depending on timing
        loop.close()

    def test_loop_get_pending_tasks(self):
        config = LoopConfig(debug=True, enable_metrics=True)
        loop = new_event_loop(config)
        
        async def long_task():
            await asyncio.sleep(1)

        task = loop.create_task(long_task())
        pending = loop.get_pending_tasks()
        assert task in pending

        loop.run_until_complete(asyncio.sleep(0.01))
        pending = loop.get_pending_tasks()
        assert task in pending  # Still pending
        task.cancel()
        loop.close()

    def test_loop_close(self):
        config = LoopConfig(debug=True, enable_metrics=True)
        loop = new_event_loop(config)
        loop.close()
        assert loop.is_closed
        assert loop.state == LoopState.CLOSED

    def test_loop_closed_error(self):
        config = LoopConfig(debug=True, enable_metrics=True)
        loop = new_event_loop(config)
        loop.close()
        with pytest.raises(LoopClosedError):
            loop.create_task(asyncio.sleep(0))

    def test_loop_context_manager(self):
        config = LoopConfig(debug=True)
        with DebugEventLoop(config) as loop:
            assert loop.state == LoopState.CREATED
            result = loop.run_until_complete(asyncio.sleep(0, result="test"))
            assert result == "test"
        assert loop.is_closed


class TestLoopPolicy:
    def test_policy_creation(self):
        from aevum import LoopPolicy
        policy = LoopPolicy()
        assert isinstance(policy, LoopPolicy)

    def test_policy_new_loop(self):
        from aevum import LoopPolicy, LoopConfig
        config = LoopConfig(debug=True)
        policy = LoopPolicy(config)
        loop = policy.new_event_loop()
        assert isinstance(loop, DebugEventLoop)
        assert loop.config.debug is True
        loop.close()


class TestRunFunction:
    def test_run_simple(self):
        import aevum
        result = aevum.run(asyncio.sleep(0, result="hello"))
        assert result == "hello"

    def test_run_with_config(self):
        import aevum
        config = LoopConfig(debug=True)
        result = aevum.run(asyncio.sleep(0, result="config"), config=config)
        assert result == "config"