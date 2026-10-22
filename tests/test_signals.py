"""
Tests for Aevum Signals
"""

import asyncio
import signal
import pytest

from aevum import (
    SignalHandler,
    GracefulShutdown,
    handle_signal,
    ignore_signal,
    get_graceful_shutdown,
    wait_for_shutdown,
    setup_signal_handlers,
)


class TestSignalHandler:
    @pytest.fixture
    def handler(self):
        return SignalHandler()

    @pytest.mark.asyncio
    async def test_add_remove_handler(self, handler):
        called = []

        def sync_handler(signum, frame):
            called.append(signum)

        handler.add_handler(signal.SIGUSR1, sync_handler)
        assert handler.remove_handler(signal.SIGUSR1, sync_handler)
        assert not handler.remove_handler(signal.SIGUSR1, sync_handler)

    @pytest.mark.asyncio
    async def test_async_handler(self, handler):
        called = []

        async def async_handler(signum, frame):
            called.append(signum)

        handler.add_handler(signal.SIGUSR1, async_handler)
        handler._dispatch_signal(signal.SIGUSR1)

        await asyncio.sleep(0.01)
        assert signal.SIGUSR1 in called

    @pytest.mark.asyncio
    async def test_once_handler(self, handler):
        called = []

        def once_handler(signum, frame):
            called.append(signum)

        handler.add_handler(signal.SIGUSR1, once_handler, once=True)
        handler._dispatch_signal(signal.SIGUSR1)
        handler._dispatch_signal(signal.SIGUSR1)

        assert len(called) == 1

    @pytest.mark.asyncio
    async def test_priority_order(self, handler):
        order = []

        def low_priority(signum, frame):
            order.append("low")

        def high_priority(signum, frame):
            order.append("high")

        handler.add_handler(signal.SIGUSR1, low_priority, priority=0)
        handler.add_handler(signal.SIGUSR1, high_priority, priority=10)

        handler._dispatch_signal(signal.SIGUSR1)
        assert order == ["high", "low"]

    @pytest.mark.asyncio
    async def test_signal_history(self, handler):
        handler._max_history = 3

        handler._dispatch_signal(signal.SIGUSR1)
        handler._dispatch_signal(signal.SIGUSR2)
        handler._dispatch_signal(signal.SIGTERM)
        handler._dispatch_signal(signal.SIGINT)

        history = handler.get_received_signals()
        assert len(history) == 3
        assert history[-1].signum == signal.SIGINT


class TestGracefulShutdown:
    @pytest.mark.asyncio
    async def test_shutdown_stages(self):
        stages_run = []

        shutdown = GracefulShutdown(
            stages=[
                ("prepare", 0.1),
                ("drain", 0.1),
                ("shutdown", 0.1),
            ]
        )

        @shutdown.on_stage("prepare")
        async def prep():
            stages_run.append("prepare")

        @shutdown.on_stage("drain")
        async def drain():
            stages_run.append("drain")

        @shutdown.on_stage("shutdown")
        async def shut():
            stages_run.append("shutdown")

        await shutdown.initiate()
        assert stages_run == ["prepare", "drain", "shutdown"]

    @pytest.mark.asyncio
    async def test_shutdown_timeout(self):
        shutdown = GracefulShutdown(
            stages=[("slow", 0.01)],
        )

        @shutdown.on_stage("slow")
        async def slow():
            await asyncio.sleep(1)

        # Should timeout and continue
        await shutdown.initiate()
        # If we get here without hanging, timeout worked

    @pytest.mark.asyncio
    async def test_wait_for_shutdown(self):
        shutdown = GracefulShutdown()

        async def trigger():
            await asyncio.sleep(0.01)
            await shutdown.initiate()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(wait_for_shutdown())
            tg.create_task(trigger())

    @pytest.mark.asyncio
    async def test_idempotent_initiate(self):
        shutdown = GracefulShutdown()
        await shutdown.initiate()
        await shutdown.initiate()  # Should not hang or error


class TestGlobalSignalHandling:
    @pytest.mark.asyncio
    async def test_handle_signal(self):
        called = []

        async def handler(signum, frame):
            called.append(signum)

        handle_signal(signal.SIGUSR1, handler)

        # Get the handler and trigger
        sh = get_graceful_shutdown()
        sh._dispatch_signal(signal.SIGUSR1)

        await asyncio.sleep(0.01)
        assert signal.SIGUSR1 in called

    @pytest.mark.asyncio
    async def test_setup_signal_handlers(self):
        called = []

        def term_handler(signum, frame):
            called.append("term")

        def int_handler(signum, frame):
            called.append("int")

        setup_signal_handlers(sigterm=term_handler, sigint=int_handler)

        # Trigger via handler
        sh = get_graceful_shutdown()
        sh._dispatch_signal(signal.SIGTERM)
        sh._dispatch_signal(signal.SIGINT)

        await asyncio.sleep(0.01)
        assert "term" in called
        assert "int" in called