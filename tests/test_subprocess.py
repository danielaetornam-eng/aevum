"""
Tests for Aevum Subprocess
"""

import asyncio
import pytest
import sys

from aevum import (
    AsyncProcess,
    ProcessGroup,
    ProcessResult,
    run_command,
    run_command_streaming,
    subprocess_context,
)


class TestAsyncProcess:
    @pytest.mark.asyncio
    async def test_run_simple_command(self):
        result = await run_command([sys.executable, "-c", "print('hello')"])
        assert result.success
        assert b"hello" in result.stdout

    @pytest.mark.asyncio
    async def test_run_command_with_stderr(self):
        result = await run_command(
            [sys.executable, "-c", "import sys; sys.stderr.write('error')"]
        )
        assert result.success
        assert b"error" in result.stderr

    @pytest.mark.asyncio
    async def test_run_failing_command(self):
        result = await run_command([sys.executable, "-c", "import sys; sys.exit(1)"])
        assert not result.success
        assert result.returncode == 1

    @pytest.mark.asyncio
    async def test_async_process_streaming(self):
        process = AsyncProcess(
            [sys.executable, "-c", "for i in range(3): print(i); import time; time.sleep(0.01)"],
            stdout=asyncio.subprocess.PIPE,
        )
        await process.start()

        lines = []
        async for line in process.stdout_lines():
            lines.append(line)

        result = await process.wait()
        assert result.success
        assert len(lines) == 3

    @pytest.mark.asyncio
    async def test_async_process_communicate(self):
        process = AsyncProcess(
            [sys.executable, "-c", "print('output')"],
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await process.start()

        stdout, stderr = await process.communicate()
        assert b"output" in stdout

    @pytest.mark.asyncio
    async def test_process_timeout(self):
        process = AsyncProcess(
            [sys.executable, "-c", "import time; time.sleep(10)"],
        )
        await process.start()

        with pytest.raises(Exception):  # SubprocessTimeoutError
            await process.wait(timeout=0.01)

        # Process should be killed
        assert process.returncode is not None

    @pytest.mark.asyncio
    async def test_process_kill(self):
        process = AsyncProcess(
            [sys.executable, "-c", "import time; time.sleep(10)"],
        )
        await process.start()

        await process.kill()
        result = await process.wait()
        assert result.timed_out or result.returncode != 0


class TestProcessGroup:
    @pytest.mark.asyncio
    async def test_process_group(self):
        group = ProcessGroup()

        p1 = group.create_process([sys.executable, "-c", "print('p1')"])
        p2 = group.create_process([sys.executable, "-c", "print('p2')"])

        await group.start_all()
        results = await group.wait_all()

        assert len(results) == 2
        assert all(r.success for r in results)

    @pytest.mark.asyncio
    async def test_process_group_terminate(self):
        group = ProcessGroup()

        p1 = group.create_process([sys.executable, "-c", "import time; time.sleep(10)"])
        p2 = group.create_process([sys.executable, "-c", "import time; time.sleep(10)"])

        await group.start_all()
        await group.terminate_all()

        # Both should be terminated
        results = await group.wait_all()
        assert len(results) == 2


class TestSubprocessContext:
    @pytest.mark.asyncio
    async def test_subprocess_context_manager(self):
        async with subprocess_context([sys.executable, "-c", "print('context')"]) as process:
            result = await process.wait()
            assert result.success


class TestRunCommandStreaming:
    @pytest.mark.asyncio
    async def test_streaming_with_handlers(self):
        stdout_lines = []
        stderr_lines = []

        async def stdout_handler(line):
            stdout_lines.append(line)

        async def stderr_handler(line):
            stderr_lines.append(line)

        result = await run_command_streaming(
            [sys.executable, "-c", "print('out'); import sys; sys.stderr.write('err')"],
            stdout_handler=stdout_handler,
            stderr_handler=stderr_handler,
        )

        assert result.success
        assert any(b"out" in line for line in stdout_lines)
        assert any(b"err" in line for line in stderr_lines)