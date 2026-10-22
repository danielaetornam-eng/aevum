"""
Aevum Core Exceptions

Exception hierarchy for the Aevum event loop library.
"""

from __future__ import annotations

from typing import Any


class AevumError(Exception):
    """Base exception for all Aevum errors."""

    def __init__(self, message: str, *, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def __str__(self) -> str:
        if self.details:
            return f"{self.message} (details: {self.details})"
        return self.message


class LoopConfigurationError(AevumError):
    """Raised when event loop configuration is invalid."""

    def __init__(self, message: str, config_key: str | None = None, **kwargs):
        details = kwargs
        if config_key:
            details["config_key"] = config_key
        super().__init__(message, details=details)


class LoopClosedError(AevumError):
    """Raised when operating on a closed event loop."""

    def __init__(self, message: str = "Event loop is closed", *, loop_id: int | None = None):
        details = {}
        if loop_id is not None:
            details["loop_id"] = loop_id
        super().__init__(message, details=details)


class TaskError(AevumError):
    """Base exception for task-related errors."""

    def __init__(self, message: str, *, task_name: str | None = None, **kwargs):
        details = kwargs
        if task_name:
            details["task_name"] = task_name
        super().__init__(message, details=details)


class TaskCancelledError(TaskError):
    """Raised when a task is cancelled."""

    def __init__(self, message: str = "Task was cancelled", *, task_name: str | None = None):
        super().__init__(message, task_name=task_name)


class TaskTimeoutError(TaskError):
    """Raised when a task times out."""

    def __init__(
        self,
        message: str = "Task timed out",
        *,
        task_name: str | None = None,
        timeout: float | None = None,
    ):
        details = {}
        if timeout is not None:
            details["timeout"] = timeout
        super().__init__(message, task_name=task_name, **details)


class ExecutorError(AevumError):
    """Base exception for executor-related errors."""

    def __init__(self, message: str, *, executor_name: str | None = None, **kwargs):
        details = kwargs
        if executor_name:
            details["executor_name"] = executor_name
        super().__init__(message, details=details)


class ExecutorShutdownError(ExecutorError):
    """Raised when executor is shut down."""

    def __init__(self, message: str = "Executor has been shut down", *, executor_name: str | None = None):
        super().__init__(message, executor_name=executor_name)


class SubprocessError(AevumError):
    """Base exception for subprocess-related errors."""

    def __init__(
        self,
        message: str,
        *,
        command: list[str] | None = None,
        returncode: int | None = None,
        **kwargs,
    ):
        details = kwargs
        if command:
            details["command"] = command
        if returncode is not None:
            details["returncode"] = returncode
        super().__init__(message, details=details)


class SubprocessTimeoutError(SubprocessError):
    """Raised when subprocess times out."""

    def __init__(
        self,
        message: str = "Subprocess timed out",
        *,
        command: list[str] | None = None,
        timeout: float | None = None,
    ):
        details = {}
        if timeout is not None:
            details["timeout"] = timeout
        super().__init__(message, command=command, **details)


class SignalError(AevumError):
    """Base exception for signal-related errors."""

    def __init__(self, message: str, *, signum: int | None = None, **kwargs):
        details = kwargs
        if signum is not None:
            details["signum"] = signum
        super().__init__(message, details=details)


class ObservabilityError(AevumError):
    """Base exception for observability-related errors."""

    pass


class MetricsError(ObservabilityError):
    """Raised when metrics collection fails."""

    pass


class TracingError(ObservabilityError):
    """Raised when tracing operations fail."""

    pass


class HealthCheckError(ObservabilityError):
    """Raised when health check fails."""

    def __init__(self, message: str, *, check_name: str | None = None, **kwargs):
        details = kwargs
        if check_name:
            details["check_name"] = check_name
        super().__init__(message, details=details)


class ExtensionError(AevumError):
    """Base exception for extension-related errors."""

    def __init__(self, message: str, *, extension_name: str | None = None, **kwargs):
        details = kwargs
        if extension_name:
            details["extension_name"] = extension_name
        super().__init__(message, details=details)


class ExtensionNotFoundError(ExtensionError):
    """Raised when an extension is not found."""

    def __init__(self, message: str, *, extension_name: str):
        super().__init__(message, extension_name=extension_name)


class ExtensionLoadError(ExtensionError):
    """Raised when an extension fails to load."""

    def __init__(self, message: str, *, extension_name: str, cause: Exception | None = None):
        details = {}
        if cause:
            details["cause"] = str(cause)
        super().__init__(message, extension_name=extension_name, **details)