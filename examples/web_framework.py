"""
Example: High-Performance Async Web Framework

This example demonstrates building a production-ready web framework on top of Aevum,
showcasing all major features:
- Enhanced event loop with debugging
- Structured concurrency with TaskScope
- Managed thread pools for blocking I/O
- Observability (metrics, tracing, health checks)
- Graceful shutdown
- Extension system
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Awaitable

from aevum import (
    # Core
    DebugEventLoop,
    LoopConfig,
    LoopContext,
    new_event_loop,
    # Tasks
    create_task_scope,
    TaskScope,
    gather_with_concurrency,
    # Executors
    create_thread_pool,
    run_in_executor,
    # Subprocess
    run_command,
    # Signals
    GracefulShutdown,
    wait_for_shutdown,
    setup_signal_handlers,
    # Observability
    trace,
    increment,
    gauge,
    histogram,
    timing,
    measure_time,
    get_tracer,
    get_metrics,
    HealthCheckRegistry,
    LoopHealthCheck,
    MemoryHealthCheck,
    run_health_checks,
    is_healthy,
    get_logger,
    # Extensions
    Extension,
    ExtensionMetadata,
    ExtensionManager,
    Application,
    application_context,
)


# ============================================================================
# Framework Core
# ============================================================================

logger = get_logger("web_framework")


@dataclass
class Request:
    """HTTP Request representation."""
    method: str
    path: str
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""
    query_params: dict[str, str] = field(default_factory=dict)
    path_params: dict[str, str] = field(default_factory=dict)


@dataclass
class Response:
    """HTTP Response representation."""
    status: int = 200
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""
    content_type: str = "application/json"

    def to_bytes(self) -> bytes:
        """Serialize response to bytes."""
        headers = {
            "Content-Type": self.content_type,
            "Content-Length": str(len(self.body)),
            **self.headers,
        }
        header_lines = [f"HTTP/1.1 {self.status} OK"]
        header_lines.extend(f"{k}: {v}" for k, v in headers.items())
        header_lines.append("")
        return "\r\n".join(header_lines).encode() + b"\r\n" + self.body

    @classmethod
    def json(cls, data: Any, status: int = 200) -> Response:
        return cls(
            status=status,
            body=json.dumps(data).encode(),
            content_type="application/json",
        )

    @classmethod
    def text(cls, text: str, status: int = 200) -> Response:
        return cls(
            status=status,
            body=text.encode(),
            content_type="text/plain",
        )

    @classmethod
    def error(cls, message: str, status: int = 500) -> Response:
        return cls.json({"error": message}, status=status)


# Type aliases
Handler = Callable[[Request], Awaitable[Response]]
Middleware = Callable[[Request, Handler], Awaitable[Response]]


class Router:
    """Simple path-based router."""

    def __init__(self):
        self._routes: dict[str, dict[str, Handler]] = {}

    def add_route(self, method: str, path: str, handler: Handler) -> None:
        if path not in self._routes:
            self._routes[path] = {}
        self._routes[path][method.upper()] = handler

    def get(self, path: str) -> Handler | None:
        return self._routes.get(path, {}).get("GET")

    def post(self, path: str) -> Handler | None:
        return self._routes.get(path, {}).get("POST")

    def route(self, path: str, method: str) -> Handler | None:
        return self._routes.get(path, {}).get(method.upper())


class WebFramework:
    """
    High-performance async web framework built on Aevum.

    Features:
    - Async request handling with structured concurrency
    - Middleware support
    - Request/response tracing
    - Metrics collection
    - Health checks
    - Graceful shutdown
    - Thread pool for blocking operations
    """

    def __init__(
        self,
        name: str = "web-framework",
        *,
        config: LoopConfig | None = None,
        max_concurrent_requests: int = 1000,
        worker_threads: int = 4,
    ):
        self.name = name
        self._config = config or LoopConfig(
            debug=True,
            enable_metrics=True,
            enable_tracing=True,
            max_concurrent_tasks=max_concurrent_requests,
        )
        self._loop: DebugEventLoop | None = None
        self._router = Router()
        self._middlewares: list[Middleware] = []
        self._shutdown = GracefulShutdown()
        self._running = False

        # Observability
        self._tracer = get_tracer(name)
        self._metrics = get_metrics(name)

        # Thread pool for blocking I/O
        self._worker_pool = None
        self._worker_threads = worker_threads

        # Register shutdown handlers
        self._setup_shutdown()

    def _setup_shutdown(self) -> None:
        """Setup graceful shutdown handlers."""

        @self._shutdown.on_stage("prepare")
        async def prepare():
            logger.info("Preparing for shutdown...")
            self._running = False

        @self._shutdown.on_stage("drain")
        async def drain():
            logger.info("Draining connections...")
            # Wait for active requests to complete
            await asyncio.sleep(0.1)

        @self._shutdown.on_stage("shutdown")
        async def shutdown():
            logger.info("Shutting down...")
            if self._worker_pool:
                self._worker_pool.shutdown(wait=True)

        setup_signal_handlers(
            sigterm=self._shutdown.initiate,
            sigint=self._shutdown.initiate,
        )

    # -----------------------------------------------------------------------
    # Routing
    # -----------------------------------------------------------------------

    def route(self, path: str, methods: list[str] | None = None) -> Callable:
        """Decorator to register a route."""
        methods = methods or ["GET"]

        def decorator(handler: Handler) -> Handler:
            for method in methods:
                self._router.add_route(method, path, handler)
            return handler
        return decorator

    def middleware(self, middleware: Middleware) -> Middleware:
        """Register middleware."""
        self._middlewares.append(middleware)
        return middleware

    # -----------------------------------------------------------------------
    # Request Handling
    # -----------------------------------------------------------------------

    @trace("framework.handle_request")
    async def handle_request(self, request: Request) -> Response:
        """Handle a single request with middleware chain."""
        increment("requests_total", tags={"method": request.method, "path": request.path})

        async def _execute(handler: Handler) -> Response:
            with measure_time("request_duration", tags={"path": request.path}):
                return await handler(request)

        # Build middleware chain
        handler = self._router.route(request.path, request.method)
        if not handler:
            return Response.error("Not Found", 404)

        for middleware in reversed(self._middlewares):
            next_handler = handler
            handler = lambda req, mw=middleware, nh=next_handler: mw(req, nh)

        try:
            response = await _execute(handler)
            increment("responses_total", tags={"status": str(response.status)})
            return response
        except Exception as e:
            logger.exception(f"Request handling error: {e}")
            increment("errors_total", tags={"type": type(e).__name__})
            return Response.error(str(e), 500)

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Handle a single TCP connection."""
        request_id = str(uuid.uuid4())[:8]
        logger.info(f"New connection: {request_id}")

        try:
            # Parse request (simplified)
            request_line = await reader.readline()
            if not request_line:
                return

            method, path, _ = request_line.decode().strip().split(" ", 2)
            request = Request(method=method, path=path)

            # Handle request
            response = await self.handle_request(request)

            # Send response
            writer.write(response.to_bytes())
            await writer.drain()

        except Exception as e:
            logger.error(f"Connection error: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    # -----------------------------------------------------------------------
    # Server Lifecycle
    # -----------------------------------------------------------------------

    async def start(self, host: str = "127.0.0.1", port: int = 8080) -> None:
        """Start the server."""
        self._loop = new_event_loop(self._config)

        # Create worker thread pool
        self._worker_pool = create_thread_pool(
            "workers",
            max_workers=self._worker_threads,
        )

        # Register health checks
        health = HealthCheckRegistry()
        health.register(LoopHealthCheck(self._loop))
        health.register(MemoryHealthCheck())

        self._running = True
        logger.info(f"Starting {self.name} on {host}:{port}")

        server = await asyncio.start_server(
            self.handle_connection,
            host,
            port,
            loop=self._loop,
        )

        # Run server and wait for shutdown
        async with server:
            await wait_for_shutdown()
            logger.info("Shutdown signal received")
            server.close()
            await server.wait_closed()

    def run(self, host: str = "127.0.0.1", port: int = 8080) -> None:
        """Run the server (blocking)."""
        self._loop.run_until_complete(self.start(host, port))

    async def run_async(self, host: str = "127.0.0.1", port: int = 8080) -> None:
        """Run the server (async)."""
        await self.start(host, port)


# ============================================================================
# Middleware Examples
# ============================================================================

@trace("middleware.logging")
async def logging_middleware(request: Request, handler: Handler) -> Response:
    """Log requests and responses."""
    start = time.perf_counter()
    logger.info(f"Request: {request.method} {request.path}")

    response = await handler(request)

    duration = time.perf_counter() - start
    logger.info(f"Response: {response.status} ({duration:.3f}s)")
    return response


@trace("middleware.auth")
async def auth_middleware(request: Request, handler: Handler) -> Response:
    """Simple auth middleware."""
    # Check for API key
    api_key = request.headers.get("X-API-Key")
    if request.path.startswith("/admin") and api_key != "secret":
        return Response.error("Unauthorized", 401)

    return await handler(request)


@trace("middleware.rate_limit")
async def rate_limit_middleware(request: Request, handler: Handler) -> Response:
    """Rate limiting middleware (simplified)."""
    # In production, use Redis or similar
    return await handler(request)


# ============================================================================
# Blocking Operations (run in thread pool)
# ============================================================================

def blocking_database_query(query: str) -> dict:
    """Simulate a blocking database query."""
    time.sleep(0.1)  # Simulate DB latency
    return {"query": query, "results": [1, 2, 3], "rows": 3}


def blocking_file_operation(path: str) -> bytes:
    """Simulate blocking file I/O."""
    time.sleep(0.05)
    return f"Content of {path}".encode()


# ============================================================================
# Example Application
# ============================================================================

def create_app() -> WebFramework:
    """Create and configure the web application."""
    app = WebFramework(
        name="my-web-app",
        config=LoopConfig(
            debug=True,
            enable_metrics=True,
            enable_tracing=True,
            slow_callback_duration=0.05,
        ),
        max_concurrent_requests=1000,
        worker_threads=4,
    )

    # Register middleware
    app.middleware(logging_middleware)
    app.middleware(auth_middleware)
    app.middleware(rate_limit_middleware)

    # Register routes
    @app.route("/health")
    async def health(request: Request) -> Response:
        healthy = is_healthy()
        checks = await run_health_checks()
        return Response.json({
            "healthy": healthy,
            "checks": {k: v.to_dict() for k, v in checks.items()},
        })

    @app.route("/metrics")
    async def metrics(request: Request) -> Response:
        metrics_data = get_metrics("my-web-app").get_all_metrics()
        return Response.json(metrics_data)

    @app.route("/trace")
    async def trace_endpoint(request: Request) -> Response:
        # Get recent spans
        tracer = get_tracer("my-web-app")
        spans = tracer.get_spans()[-10:]
        return Response.json([s.to_dict() for s in spans])

    @app.route("/api/users")
    async def list_users(request: Request) -> Response:
        # Use thread pool for blocking DB query
        result = await run_in_executor(blocking_database_query, "SELECT * FROM users")
        return Response.json(result)

    @app.route("/api/files/{path}")
    async def get_file(request: Request) -> Response:
        file_path = request.path_params.get("path", "")
        # Use thread pool for blocking file I/O
        content = await run_in_executor(blocking_file_operation, file_path)
        return Response.text(content.decode())

    @app.route("/api/process", methods=["POST"])
    async def run_process(request: Request) -> Response:
        # Run subprocess
        result = await run_command(
            ["echo", "hello from subprocess"],
            capture_output=True,
        )
        return Response.json({
            "stdout": result.stdout.decode(),
            "stderr": result.stderr.decode(),
            "returncode": result.returncode,
        })

    @app.route("/api/concurrent")
    async def concurrent_ops(request: Request) -> Response:
        """Demonstrate structured concurrency."""
        async with create_task_scope() as scope:
            # Run multiple operations concurrently
            scope.create_task(run_in_executor(blocking_database_query, "query1"))
            scope.create_task(run_in_executor(blocking_database_query, "query2"))
            scope.create_task(run_in_executor(blocking_file_operation, "file1"))

            results = await scope.wait_all()

        return Response.json({"results": results})

    return app


# ============================================================================
# Extension Example
# ============================================================================

class MetricsExtension(Extension):
    """Extension that exports metrics to external systems."""

    metadata = ExtensionMetadata(
        name="metrics-exporter",
        version="1.0.0",
        description="Exports metrics to Prometheus",
    )

    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__(config)
        self._export_interval = config.get("export_interval", 30) if config else 30
        self._export_task: asyncio.Task | None = None

    def on_loop_start(self) -> None:
        if self.loop:
            self._export_task = self.loop.create_task(
                self._export_loop(), name="metrics-exporter"
            )

    def on_loop_shutdown(self, loop: DebugEventLoop) -> None:
        if self._export_task:
            self._export_task.cancel()

    async def _export_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._export_interval)
                metrics = get_metrics().export_prometheus()
                logger.info(f"Exporting metrics:\n{metrics}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Metrics export failed: {e}")


# ============================================================================
# Main Entry Point
# ============================================================================

async def main():
    """Main entry point."""
    # Create app with extensions
    async with application_context("my-web-app") as framework_app:
        # Load extensions
        framework_app.extensions.load_extension(
            "metrics-exporter",
            MetricsExtension,
            {"export_interval": 10},
        )

        # Create our web framework
        web_app = create_app()

        # Mount web framework routes
        for path, methods in web_app._router._routes.items():
            for method, handler in methods.items():
                framework_app.extensions._loop.create_task(
                    lambda: None, name=f"route-{method}-{path}"
                )

        logger.info("Application ready")

        # Run the server
        await web_app.run_async("127.0.0.1", 8080)


if __name__ == "__main__":
    # Simple run
    app = create_app()
    app.run("127.0.0.1", 8080)