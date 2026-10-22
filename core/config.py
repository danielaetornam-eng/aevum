"""
Event Loop Configuration

Configuration classes for the enhanced event loop.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
import asyncio


@dataclass(slots=True)
class LoopConfig:
    """
    Configuration for DebugEventLoop.

    All settings are optional and have sensible defaults.
    """

    # Debug settings
    debug: bool = False
    slow_callback_duration: float = 0.1
    slow_task_duration: float = 1.0

    # Task management
    task_factory: type[asyncio.Task] | None = None
    default_task_timeout: float | None = None
    max_concurrent_tasks: int | None = None

    # Exception handling
    exception_handler: callable | None = None
    unhandled_exception_log_level: str = "ERROR"

    # Metrics and monitoring
    enable_metrics: bool = True
    metrics_interval: float = 10.0
    metrics_history_size: int = 1000

    # Tracing
    enable_tracing: bool = False
    trace_slow_callbacks: bool = True
    trace_task_creation: bool = True

    # Resource limits
    max_fds: int | None = None
    max_timers: int | None = None

    # Thread pool
    default_executor: str | None = "thread"
    default_executor_max_workers: int | None = None

    # Subprocess
    subprocess_limit: int | None = None

    # Signal handling
    handle_signals: bool = True
    signal_handlers: dict[int, callable] = field(default_factory=dict)

    # Extensions
    extensions: list[str] = field(default_factory=list)
    extension_config: dict[str, dict[str, Any]] = field(default_factory=dict)

    def __post_init__(self):
        if self.slow_callback_duration <= 0:
            raise ValueError("slow_callback_duration must be positive")
        if self.slow_task_duration <= 0:
            raise ValueError("slow_task_duration must be positive")
        if self.metrics_interval <= 0:
            raise ValueError("metrics_interval must be positive")
        if self.metrics_history_size <= 0:
            raise ValueError("metrics_history_size must be positive")
        if self.max_concurrent_tasks is not None and self.max_concurrent_tasks <= 0:
            raise ValueError("max_concurrent_tasks must be positive")
        if self.default_executor_max_workers is not None and self.default_executor_max_workers <= 0:
            raise ValueError("default_executor_max_workers must be positive")

    def to_dict(self) -> dict[str, Any]:
        """Convert config to dictionary."""
        return {
            "debug": self.debug,
            "slow_callback_duration": self.slow_callback_duration,
            "slow_task_duration": self.slow_task_duration,
            "task_factory": self.task_factory.__name__ if self.task_factory else None,
            "default_task_timeout": self.default_task_timeout,
            "max_concurrent_tasks": self.max_concurrent_tasks,
            "exception_handler": self.exception_handler.__name__ if self.exception_handler else None,
            "unhandled_exception_log_level": self.unhandled_exception_log_level,
            "enable_metrics": self.enable_metrics,
            "metrics_interval": self.metrics_interval,
            "metrics_history_size": self.metrics_history_size,
            "enable_tracing": self.enable_tracing,
            "trace_slow_callbacks": self.trace_slow_callbacks,
            "trace_task_creation": self.trace_task_creation,
            "max_fds": self.max_fds,
            "max_timers": self.max_timers,
            "default_executor": self.default_executor,
            "default_executor_max_workers": self.default_executor_max_workers,
            "subprocess_limit": self.subprocess_limit,
            "handle_signals": self.handle_signals,
            "signal_handlers": list(self.signal_handlers.keys()),
            "extensions": self.extensions,
            "extension_config": self.extension_config,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LoopConfig:
        """Create config from dictionary."""
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass(slots=True)
class LoopMetrics:
    """
    Runtime metrics for the event loop.

    Collected automatically when enable_metrics is True.
    """

    # Loop statistics
    loop_iterations: int = 0
    callbacks_executed: int = 0
    callbacks_scheduled: int = 0

    # Timing
    total_callback_time: float = 0.0
    total_idle_time: float = 0.0
    loop_start_time: float = 0.0
    last_iteration_time: float = 0.0

    # Task statistics
    tasks_created: int = 0
    tasks_completed: int = 0
    tasks_cancelled: int = 0
    tasks_failed: int = 0
    tasks_pending: int = 0

    # Slow callbacks
    slow_callbacks: int = 0
    slowest_callback_time: float = 0.0
    slowest_callback_name: str = ""

    # Timer statistics
    timers_scheduled: int = 0
    timers_fired: int = 0
    timers_cancelled: int = 0

    # I/O statistics
    io_reads: int = 0
    io_writes: int = 0
    io_errors: int = 0

    # Exception statistics
    exceptions_handled: int = 0
    exceptions_unhandled: int = 0

    # Resource usage
    peak_memory_mb: float = 0.0
    current_memory_mb: float = 0.0
    open_fds: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            "loop_iterations": self.loop_iterations,
            "callbacks_executed": self.callbacks_executed,
            "callbacks_scheduled": self.callbacks_scheduled,
            "total_callback_time": self.total_callback_time,
            "total_idle_time": self.total_idle_time,
            "loop_start_time": self.loop_start_time,
            "last_iteration_time": self.last_iteration_time,
            "tasks_created": self.tasks_created,
            "tasks_completed": self.tasks_completed,
            "tasks_cancelled": self.tasks_cancelled,
            "tasks_failed": self.tasks_failed,
            "tasks_pending": self.tasks_pending,
            "slow_callbacks": self.slow_callbacks,
            "slowest_callback_time": self.slowest_callback_time,
            "slowest_callback_name": self.slowest_callback_name,
            "timers_scheduled": self.timers_scheduled,
            "timers_fired": self.timers_fired,
            "timers_cancelled": self.timers_cancelled,
            "io_reads": self.io_reads,
            "io_writes": self.io_writes,
            "io_errors": self.io_errors,
            "exceptions_handled": self.exceptions_handled,
            "exceptions_unhandled": self.exceptions_unhandled,
            "peak_memory_mb": self.peak_memory_mb,
            "current_memory_mb": self.current_memory_mb,
            "open_fds": self.open_fds,
        }

    def reset(self) -> None:
        """Reset all metrics to zero."""
        for field_name in self.__dataclass_fields__:
            setattr(self, field_name, 0 if field_name != "slowest_callback_name" else "")
        self.loop_start_time = 0.0
        self.last_iteration_time = 0.0


class LoopState:
    """Event loop state enumeration."""

    CREATED = "created"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    CLOSED = "closed"
    ERROR = "error"

    @classmethod
    def all(cls) -> list[str]:
        return [
            cls.CREATED,
            cls.RUNNING,
            cls.STOPPING,
            cls.STOPPED,
            cls.CLOSED,
            cls.ERROR,
        ]

    @classmethod
    def is_terminal(cls, state: str) -> bool:
        return state in (cls.STOPPED, cls.CLOSED, cls.ERROR)