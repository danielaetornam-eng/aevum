"""
Aevum - God-tier asyncio extension library for building async frameworks.

Aevum provides a comprehensive set of tools for building production-grade
asynchronous applications and frameworks on top of Python's asyncio.

Core Features:
- Enhanced event loop with debugging, metrics, and tracing
- Advanced task management with structured concurrency
- Managed thread/process pool executors
- Async subprocess management with streaming
- Signal handling and graceful shutdown
- Observability: metrics, distributed tracing, health checks
- Extension framework for building custom frameworks

Example:
    import aevum
    from aevum import run, create_task_scope

    async def main():
        async with create_task_scope() as scope:
            scope.create_task(do_work())
            scope.create_task(do_other_work())

    aevum.run(main())
"""

from __future__ import annotations

from .core import (
    DebugEventLoop,
    LoopConfig,
    LoopMetrics,
    LoopState,
    LoopPolicy,
    get_policy,
    set_policy,
    LoopContext,
    current_loop,
    set_current_loop,
    new_event_loop,
    get_event_loop,
    configure_default_loop,
    AevumError,
    LoopClosedError,
    LoopConfigurationError,
)
from .core.exceptions import (
    ExtensionError,
    ExtensionNotFoundError,
    ExtensionLoadError,
)
from .tasks import (
    TaskScope,
    SupervisedTaskGroup,
    CancellationScope,
    TaskMonitor,
    enable_task_monitoring,
    create_task_scope,
    supervised_task_group,
    gather_with_concurrency,
    wait_for_first,
    wait_for_all,
    get_cancellation_scope,
    set_task_local,
    get_task_local,
    clear_task_local,
)
from .executors import (
    ManagedThreadPoolExecutor,
    ProcessPoolExecutor,
    ExecutorManager,
    get_executor_manager,
    create_thread_pool,
    create_process_pool,
    run_in_executor,
    executor_context,
)
from .subprocess import (
    AsyncProcess,
    ProcessGroup,
    ProcessResult,
    run_command,
    run_command_streaming,
    subprocess_context,
)
from .signals import (
    SignalHandler,
    GracefulShutdown,
    handle_signal,
    ignore_signal,
    default_signal_handler,
    setup_signal_handlers,
    get_graceful_shutdown,
    wait_for_shutdown,
    shutdown_context,
)
from .observability import (
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
    HealthCheck,
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
from .ext import (
    Extension,
    ExtensionMetadata,
    ExtensionManager,
    ConfigManager,
    get_config_manager,
    Plugin,
    Application,
    application_context,
    load_extensions_from_entry_points,
)

__version__ = "1.0.0"

__all__ = [
    # Core
    "DebugEventLoop",
    "LoopConfig",
    "LoopMetrics",
    "LoopState",
    "LoopPolicy",
    "get_policy",
    "set_policy",
    "LoopContext",
    "current_loop",
    "set_current_loop",
    "new_event_loop",
    "get_event_loop",
    "configure_default_loop",
    "AevumError",
    "LoopClosedError",
    "LoopConfigurationError",
    "ExtensionError",
    "ExtensionNotFoundError",
    "ExtensionLoadError",
    # Tasks
    "TaskScope",
    "SupervisedTaskGroup",
    "CancellationScope",
    "TaskMonitor",
    "enable_task_monitoring",
    "create_task_scope",
    "supervised_task_group",
    "gather_with_concurrency",
    "wait_for_first",
    "wait_for_all",
    "get_cancellation_scope",
    "set_task_local",
    "get_task_local",
    "clear_task_local",
    # Executors
    "ManagedThreadPoolExecutor",
    "ProcessPoolExecutor",
    "ExecutorManager",
    "get_executor_manager",
    "create_thread_pool",
    "create_process_pool",
    "run_in_executor",
    "executor_context",
    # Subprocess
    "AsyncProcess",
    "ProcessGroup",
    "ProcessResult",
    "run_command",
    "run_command_streaming",
    "subprocess_context",
    # Signals
    "SignalHandler",
    "GracefulShutdown",
    "handle_signal",
    "ignore_signal",
    "default_signal_handler",
    "setup_signal_handlers",
    "get_graceful_shutdown",
    "wait_for_shutdown",
    "shutdown_context",
    # Observability
    "Tracer",
    "Span",
    "get_tracer",
    "trace",
    "MetricsCollector",
    "get_metrics",
    "increment",
    "gauge",
    "histogram",
    "timing",
    "measure_time",
    "HealthCheck",
    "HealthCheckRegistry",
    "LoopHealthCheck",
    "MemoryHealthCheck",
    "DiskHealthCheck",
    "get_health_registry",
    "run_health_checks",
    "is_healthy",
    "HealthCheckResult",
    "StructuredLogger",
    "get_logger",
    # Extensions
    "Extension",
    "ExtensionMetadata",
    "ExtensionManager",
    "ConfigManager",
    "get_config_manager",
    "Plugin",
    "Application",
    "application_context",
    "load_extensions_from_entry_points",
]

# Convenience function
def run(coro, *, config: LoopConfig | None = None) -> Any:
    """Run a coroutine with aevum event loop."""
    loop = new_event_loop(config)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()