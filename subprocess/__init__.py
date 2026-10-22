"""
Async Subprocess Management

Provides high-level async subprocess management with:
- Streaming stdout/stderr
- Timeout handling
- Process groups
- Resource limits
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, BinaryIO

from ..core.exceptions import SubprocessError, SubprocessTimeoutError
from ..core.loop import DebugEventLoop, get_event_loop
from ..tasks import TaskScope

if sys.version_info >= (3, 11):
    from asyncio import SubprocessTransport, SubprocessProtocol
else:
    from asyncio import SubprocessTransport, SubprocessProtocol


@dataclass(slots=True)
class ProcessResult:
    """Result of a completed process."""

    returncode: int
    stdout: bytes
    stderr: bytes
    pid: int
    duration: float
    timed_out: bool = False

    @property
    def success(self) -> bool:
        return self.returncode == 0 and not self.timed_out

    def __bool__(self) -> bool:
        return self.success


@dataclass(slots=True)
class ProcessInfo:
    """Information about a running process."""

    pid: int
    cmd: list[str]
    start_time: float
    stdout_chunks: list[bytes] = field(default_factory=list)
    stderr_chunks: list[bytes] = field(default_factory=list)
    returncode: int | None = None
    completed: bool = False

    @property
    def duration(self) -> float:
        import time
        return time.monotonic() - self.start_time


class AsyncProcess:
    """
    Async wrapper around subprocess with streaming support.

    Features:
    - Async iteration over stdout/stderr lines
    - Timeout handling
    - Signal forwarding
    - Resource monitoring
    """

    def __init__(
        self,
        cmd: list[str] | str,
        *,
        loop: DebugEventLoop | None = None,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        stdin: BinaryIO | int | None = None,
        stdout: BinaryIO | int | None = asyncio.subprocess.PIPE,
        stderr: BinaryIO | int | None = asyncio.subprocess.PIPE,
        limit: int = 64 * 1024,
        start_new_session: bool = False,
        pass_fds: tuple[int, ...] = (),
    ):
        self._loop = loop or get_event_loop()
        self._cmd = cmd if isinstance(cmd, list) else [cmd]
        self._cwd = cwd
        self._env = env
        self._stdin = stdin
        self._stdout = stdout
        self._stderr = stderr
        self._limit = limit
        self._start_new_session = start_new_session
        self._pass_fds = pass_fds

        self._process: asyncio.subprocess.Process | None = None
        self._info = ProcessInfo(
            pid=0,
            cmd=self._cmd,
            start_time=0,
        )
        self._stdout_queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._stderr_queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._stdout_task: asyncio.Task | None = None
        self._stderr_task: asyncio.Task | None = None

    @property
    def pid(self) -> int | None:
        return self._process.pid if self._process else None

    @property
    def returncode(self) -> int | None:
        return self._process.returncode if self._process else None

    @property
    def info(self) -> ProcessInfo:
        return self._info

    async def start(self) -> None:
        """Start the process."""
        if self._process is not None:
            raise SubprocessError("Process already started", command=self._cmd)

        self._info.start_time = asyncio.get_event_loop().time()

        self._process = await asyncio.create_subprocess_exec(
            *self._cmd,
            cwd=self._cwd,
            env=self._env,
            stdin=self._stdin,
            stdout=self._stdout,
            stderr=self._stderr,
            limit=self._limit,
            start_new_session=self._start_new_session,
            pass_fds=self._pass_fds,
            loop=self._loop,
        )

        self._info.pid = self._process.pid or 0

        # Start streaming tasks
        if self._process.stdout:
            self._stdout_task = self._loop.create_task(
                self._read_stream(self._process.stdout, self._stdout_queue),
                name=f"stdout-reader-{self._info.pid}",
            )
        if self._process.stderr:
            self._stderr_task = self._loop.create_task(
                self._read_stream(self._process.stderr, self._stderr_queue),
                name=f"stderr-reader-{self._info.pid}",
            )

    async def _read_stream(
        self,
        stream: asyncio.StreamReader,
        queue: asyncio.Queue[bytes],
    ) -> None:
        """Read from stream and put in queue."""
        try:
            while True:
                line = await stream.readline()
                if not line:
                    break
                await queue.put(line)
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
        finally:
            await queue.put(b"")  # EOF marker

    async def wait(self, timeout: float | None = None) -> ProcessResult:
        """Wait for process to complete."""
        if self._process is None:
            await self.start()

        try:
            if timeout is not None:
                returncode = await asyncio.wait_for(self._process.wait(), timeout=timeout)
            else:
                returncode = await self._process.wait()
        except asyncio.TimeoutError:
            await self.kill()
            raise SubprocessTimeoutError(
                f"Process timed out after {timeout}s",
                command=self._cmd,
                timeout=timeout,
            )

        self._info.returncode = returncode
        self._info.completed = True

        # Collect remaining output
        stdout_data = b"".join(self._info.stdout_chunks)
        stderr_data = b"".join(self._info.stderr_chunks)

        return ProcessResult(
            returncode=returncode,
            stdout=stdout_data,
            stderr=stderr_data,
            pid=self._info.pid,
            duration=self._info.duration,
        )

    async def communicate(
        self,
        input: bytes | None = None,
        timeout: float | None = None,
    ) -> tuple[bytes, bytes]:
        """Send input and wait for completion."""
        if self._process is None:
            await self.start()

        try:
            if timeout is not None:
                stdout, stderr = await asyncio.wait_for(
                    self._process.communicate(input), timeout=timeout
                )
            else:
                stdout, stderr = await self._process.communicate(input)
        except asyncio.TimeoutError:
            await self.kill()
            raise SubprocessTimeoutError(
                f"Process timed out after {timeout}s",
                command=self._cmd,
                timeout=timeout,
            )

        self._info.returncode = self._process.returncode
        self._info.completed = True

        return stdout or b"", stderr or b""

    def stdout_lines(self) -> AsyncIterator[bytes]:
        """Async iterator over stdout lines."""
        async def _iter():
            while True:
                line = await self._stdout_queue.get()
                if not line:
                    break
                self._info.stdout_chunks.append(line)
                yield line

        return _iter()

    def stderr_lines(self) -> AsyncIterator[bytes]:
        """Async iterator over stderr lines."""
        async def _iter():
            while True:
                line = await self._stderr_queue.get()
                if not line:
                    break
                self._info.stderr_chunks.append(line)
                yield line

        return _iter()

    async def send_signal(self, sig: signal.Signals | int) -> None:
        """Send signal to process."""
        if self._process and self._process.pid:
            self._process.send_signal(sig)

    async def terminate(self) -> None:
        """Terminate the process."""
        if self._process:
            self._process.terminate()

    async def kill(self) -> None:
        """Kill the process."""
        if self._process:
            self._process.kill()

    async def close(self) -> None:
        """Clean up resources."""
        if self._stdout_task:
            self._stdout_task.cancel()
        if self._stderr_task:
            self._stderr_task.cancel()


class ProcessGroup:
    """
    Group of related processes with coordinated lifecycle.

    Features:
    - Start/stop all processes together
    - Shared signal handling
    - Collective timeout
    """

    def __init__(
        self,
        *,
        loop: DebugEventLoop | None = None,
        name: str = "ProcessGroup",
    ):
        self._loop = loop or get_event_loop()
        self._name = name
        self._processes: list[AsyncProcess] = []
        self._running = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def processes(self) -> list[AsyncProcess]:
        return self._processes

    def add_process(self, process: AsyncProcess) -> None:
        """Add a process to the group."""
        self._processes.append(process)

    def create_process(self, *args: Any, **kwargs: Any) -> AsyncProcess:
        """Create and add a process."""
        process = AsyncProcess(*args, **kwargs, loop=self._loop)
        self._processes.append(process)
        return process

    async def start_all(self) -> None:
        """Start all processes."""
        if self._running:
            return
        self._running = True
        for process in self._processes:
            await process.start()

    async def wait_all(self, timeout: float | None = None) -> list[ProcessResult]:
        """Wait for all processes to complete."""
        async with TaskScope(loop=self._loop, timeout=timeout) as scope:
            for process in self._processes:
                scope.create_task(process.wait(timeout))
            results = await scope.wait_all()
        return results

    async def terminate_all(self) -> None:
        """Terminate all processes."""
        for process in self._processes:
            await process.terminate()

    async def kill_all(self) -> None:
        """Kill all processes."""
        for process in self._processes:
            await process.kill()

    async def close_all(self) -> None:
        """Close all processes."""
        for process in self._processes:
            await process.close()
        self._running = False


async def run_command(
    cmd: list[str] | str,
    *,
    loop: DebugEventLoop | None = None,
    timeout: float | None = None,
    cwd: str | None = None,
    env: dict[str, str] | None = None,
    input: bytes | None = None,
    capture_output: bool = True,
) -> ProcessResult:
    """
    Run a command and wait for completion.

    High-level convenience function.
    """
    process = AsyncProcess(
        cmd,
        loop=loop,
        cwd=cwd,
        env=env,
        stdin=asyncio.subprocess.PIPE if input else None,
        stdout=asyncio.subprocess.PIPE if capture_output else None,
        stderr=asyncio.subprocess.PIPE if capture_output else None,
    )
    await process.start()
    result = await process.wait(timeout)
    return result


async def run_command_streaming(
    cmd: list[str] | str,
    *,
    loop: DebugEventLoop | None = None,
    timeout: float | None = None,
    cwd: str | None = None,
    env: dict[str, str] | None = None,
    stdout_handler: Callable[[bytes], Any] | None = None,
    stderr_handler: Callable[[bytes], Any] | None = None,
) -> ProcessResult:
    """
    Run a command with streaming output handlers.
    """
    process = AsyncProcess(
        cmd,
        loop=loop,
        cwd=cwd,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await process.start()

    async def handle_stream(stream_iter, handler):
        if handler is None:
            return
        async for line in stream_iter:
            await handler(line) if asyncio.iscoroutinefunction(handler) else handler(line)

    async with TaskScope(loop=loop, timeout=timeout) as scope:
        if stdout_handler:
            scope.create_task(handle_stream(process.stdout_lines(), stdout_handler))
        if stderr_handler:
            scope.create_task(handle_stream(process.stderr_lines(), stderr_handler))
        result = await process.wait(timeout)

    return result


@asynccontextmanager
async def subprocess_context(
    cmd: list[str] | str,
    *,
    loop: DebugEventLoop | None = None,
    **kwargs: Any,
) -> AsyncIterator[AsyncProcess]:
    """Context manager for subprocess lifecycle."""
    process = AsyncProcess(cmd, loop=loop, **kwargs)
    await process.start()
    try:
        yield process
    finally:
        await process.close()