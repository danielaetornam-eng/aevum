# Aevum - God-tier Asyncio Extension Library

> **Aevum** (Latin for "age, eternity") - A comprehensive, production-ready asyncio extension library for building high-performance async frameworks and applications.

[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Overview

Aevum extends Python's `asyncio` with enterprise-grade features for building robust, observable, and maintainable asynchronous applications:

- **Enhanced Event Loop** - Debugging, metrics, tracing, slow callback detection
- **Structured Concurrency** - TaskScope, SupervisedTaskGroup, CancellationScope
- **Managed Executors** - Thread/process pools with lifecycle, health checks, auto-scaling
- **Async Subprocess** - Streaming I/O, timeouts, process groups
- **Signal Handling** - Graceful shutdown, multi-stage shutdown, signal propagation
- **Observability** - Metrics (Prometheus), distributed tracing, health checks
- **Extension Framework** - Plugin system, configuration management, hot reload

## Installation

```bash
pip install aevum
```

For development:
```bash
pip install -e ".[dev]"
```

## Quick Start

```python
import aevum
import asyncio

async def main():
    async with aevum.create_task_scope() as scope:
        scope.create_task(asyncio.sleep(1, result="task 1"))
        scope.create_task(asyncio.sleep(2, result="task 2"))
        results = await scope.wait_all()
        print(results)  # ["task 1", "task 2"]

aevum.run(main())
```

## Core Concepts

### Enhanced Event Loop

```python
from aevum import DebugEventLoop, LoopConfig, LoopContext

config = LoopConfig(
    debug=True,
    enable_metrics=True,
    enable_tracing=True,
    slow_callback_duration=0.1,
    slow_task_duration=1.0,
)

with LoopContext(config=config) as loop:
    loop.run_until_complete(main())
```

### Structured Concurrency

```python
from aevum import create_task_scope, supervised_task_group, CancellationScope

# Automatic cleanup on exit
async with create_task_scope(timeout=5.0) as scope:
    scope.create_task(do_work())
    scope.create_task(do_other_work())
    results = await scope.wait_all()

# Supervised tasks with auto-restart
async with supervised_task_group(restart_on_failure=True, max_restarts=3) as group:
    group.create_task(unreliable_service(), name="service")

# Explicit cancellation control
with CancellationScope() as scope:
    task = create_task(long_operation())
    if should_cancel:
        scope.cancel()
```

### Managed Executors

```python
from aevum import create_thread_pool, create_process_pool, run_in_executor

# Thread pool for blocking I/O
pool = create_thread_pool("db-pool", max_workers=10)

# Run blocking code without blocking event loop
result = await run_in_executor(blocking_db_query, "SELECT * FROM users")

# Process pool for CPU-bound work
proc_pool = create_process_pool("cpu-pool", max_workers=4)
result = await proc_pool.run_in_executor(heavy_computation, data)
```

### Observability

```python
from aevum import trace, increment, histogram, timing, measure_time, run_health_checks

# Distributed tracing
@trace("user.login")
async def login(user_id: str):
    with measure_time("db.lookup"):
        user = await db.get_user(user_id)
    return user

# Metrics
increment("requests_total", tags={"endpoint": "/api/users"})
histogram("request_latency", 0.123, tags={"endpoint": "/api/users"})
timing("db_query", 0.045)

# Health checks
checks = await run_health_checks()
if not all(c.healthy for c in checks.values()):
    alert_oncall()
```

### Graceful Shutdown

```python
from aevum import GracefulShutdown, wait_for_shutdown

shutdown = GracefulShutdown(
    stages=[
        ("prepare", 5.0),    # Stop accepting new requests
        ("drain", 30.0),     # Finish in-flight requests
        ("shutdown", 10.0),  # Close connections, flush buffers
        ("force", 5.0),      # Force terminate
    ]
)

@shutdown.on_stage("drain")
async def drain_connections():
    await connection_pool.drain()

# In your main loop
await wait_for_shutdown()
await shutdown.initiate()
```

## Architecture

```
aevum/
├── core/           # Event loop, config, policy, context
├── tasks/          # Structured concurrency, task management
├── executors/      # Thread/process pool management
├── subprocess/     # Async subprocess with streaming
├── signals/        # Signal handling, graceful shutdown
├── observability/  # Metrics, tracing, health checks
├── ext/            # Extension framework, plugins
└── examples/       # Example frameworks
```

## Building Frameworks

Aevum provides `Application` base class for building frameworks:

```python
from aevum import Application, Extension, ExtensionMetadata

class MyExtension(Extension):
    metadata = ExtensionMetadata(name="my-ext", version="1.0.0")
    
    def on_loop_init(self, loop):
        # Setup resources
        pass
    
    def on_loop_shutdown(self, loop):
        # Cleanup resources
        pass

app = Application("my-framework")
app.extensions.load_extension("my-ext", MyExtension, {"config": "value"})
await app.run()
```

## Running Tests

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest aevum/tests -v

# With coverage
pytest aevum/tests --cov=aevum --cov-report=html
```

## Examples

- `examples/web_framework.py` - Production web framework with middleware, routing, health checks
- `examples/advanced_patterns.py` - Job queues, event streams, resource pools, circuit breakers

## Requirements

- Python 3.11+
- psutil (for system metrics)

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `pytest`
5. Submit a PR

## Roadmap

- [ ] WebSocket support
- [ ] HTTP/2 client/server
- [ ] Distributed tracing exporters (Jaeger, Zipkin)
- [ ] Metrics exporters (Prometheus, StatsD, DataDog)
- [ ] Built-in rate limiting, circuit breakers
- [ ] Cluster-aware extensions
- [ ] GraphQL subscription support