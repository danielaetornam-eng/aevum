"""
Extension Framework

Provides extensibility APIs for building async frameworks on top of Aevum:
- Extension base classes
- Plugin system
- Configuration management
- Lifecycle hooks
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, TypeVar

from ..core.config import LoopConfig
from ..core.exceptions import ExtensionError, ExtensionLoadError, ExtensionNotFoundError
from ..core.loop import DebugEventLoop

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="Extension")


@dataclass(slots=True)
class ExtensionMetadata:
    """Metadata for an extension."""

    name: str
    version: str = "1.0.0"
    description: str = ""
    author: str = ""
    dependencies: list[str] = field(default_factory=list)
    config_schema: dict[str, Any] = field(default_factory=dict)


class Extension(ABC):
    """
    Base class for Aevum extensions.

    Extensions can:
    - Hook into loop lifecycle events
    - Provide custom functionality
    - Integrate with metrics/tracing/health checks
    """

    metadata: ExtensionMetadata

    def __init__(self, config: dict[str, Any] | None = None):
        self._config = config or {}
        self._loop: DebugEventLoop | None = None
        self._started = False

    @property
    def name(self) -> str:
        return self.metadata.name

    @property
    def config(self) -> dict[str, Any]:
        return self._config

    @property
    def loop(self) -> DebugEventLoop | None:
        return self._loop

    def on_loop_init(self, loop: DebugEventLoop) -> None:
        """Called when extension is registered with a loop."""
        self._loop = loop
        logger.info(f"Extension '{self.name}' initialized")

    def on_loop_start(self) -> None:
        """Called when loop starts running."""
        self._started = True
        logger.debug(f"Extension '{self.name}' started")

    def on_loop_stop(self) -> None:
        """Called when loop stops."""
        logger.debug(f"Extension '{self.name}' stopped")

    def on_loop_shutdown(self, loop: DebugEventLoop) -> None:
        """Called when loop is shutting down."""
        logger.info(f"Extension '{self.name}' shutting down")

    def on_task_created(self, task: asyncio.Task) -> None:
        """Called when a task is created."""
        pass

    def on_task_done(self, task: asyncio.Task) -> None:
        """Called when a task completes."""
        pass

    def on_signal(self, signum: int, frame: Any) -> None:
        """Called when a signal is received."""
        pass

    def get_health_checks(self) -> list[Any]:
        """Return health checks provided by this extension."""
        return []

    def get_metrics_exporters(self) -> list[Callable]:
        """Return metrics exporters provided by this extension."""
        return []

    def get_span_exporters(self) -> list[Callable]:
        """Return span exporters provided by this extension."""
        return []


class ExtensionManager:
    """
    Manages extensions for an event loop.

    Features:
    - Dependency resolution
    - Configuration validation
    - Lifecycle management
    - Hot reload support
    """

    def __init__(self, loop: DebugEventLoop):
        self._loop = loop
        self._extensions: dict[str, Extension] = {}
        self._metadata: dict[str, ExtensionMetadata] = {}
        self._load_order: list[str] = []

    def load_extension(
        self,
        name: str,
        extension_class: type[Extension],
        config: dict[str, Any] | None = None,
    ) -> Extension:
        """Load and register an extension."""
        if name in self._extensions:
            raise ExtensionError(f"Extension '{name}' already loaded", extension_name=name)

        # Check dependencies
        metadata = getattr(extension_class, "metadata", ExtensionMetadata(name=name))
        for dep in metadata.dependencies:
            if dep not in self._extensions:
                raise ExtensionError(
                    f"Extension '{name}' depends on '{dep}' which is not loaded",
                    extension_name=name,
                )

        # Create instance
        try:
            extension = extension_class(config)
        except Exception as e:
            raise ExtensionLoadError(f"Failed to create extension '{name}': {e}", extension_name=name, cause=e)

        # Validate config
        if metadata.config_schema:
            self._validate_config(extension, metadata.config_schema)

        # Register
        self._extensions[name] = extension
        self._metadata[name] = metadata
        self._load_order.append(name)

        # Initialize with loop
        extension.on_loop_init(self._loop)

        logger.info(f"Loaded extension: {name} v{metadata.version}")
        return extension

    def _validate_config(self, extension: Extension, schema: dict[str, Any]) -> None:
        """Validate extension configuration."""
        # Simple validation - in production use jsonschema
        for key, spec in schema.items():
            if spec.get("required", False) and key not in extension.config:
                raise ExtensionError(
                    f"Required config '{key}' missing for extension '{extension.name}'",
                    extension_name=extension.name,
                )

    def unload_extension(self, name: str) -> bool:
        """Unload an extension."""
        extension = self._extensions.pop(name, None)
        if extension is None:
            return False

        # Check if other extensions depend on this
        for ext_name, metadata in self._metadata.items():
            if name in metadata.dependencies:
                raise ExtensionError(
                    f"Cannot unload '{name}': required by '{ext_name}'",
                    extension_name=name,
                )

        extension.on_loop_shutdown(self._loop)
        self._metadata.pop(name, None)
        self._load_order.remove(name)

        logger.info(f"Unloaded extension: {name}")
        return True

    def get_extension(self, name: str) -> Extension:
        """Get an extension by name."""
        if name not in self._extensions:
            raise ExtensionNotFoundError(f"Extension '{name}' not found", extension_name=name)
        return self._extensions[name]

    def has_extension(self, name: str) -> bool:
        return name in self._extensions

    def list_extensions(self) -> list[str]:
        return self._load_order.copy()

    def get_metadata(self, name: str) -> ExtensionMetadata | None:
        return self._metadata.get(name)

    async def start_all(self) -> None:
        """Start all extensions."""
        for name in self._load_order:
            self._extensions[name].on_loop_start()

    async def stop_all(self) -> None:
        """Stop all extensions."""
        for name in reversed(self._load_order):
            self._extensions[name].on_loop_stop()

    async def shutdown_all(self) -> None:
        """Shutdown all extensions."""
        for name in reversed(self._load_order):
            self._extensions[name].on_loop_shutdown(self._loop)


def load_extensions_from_entry_points(group: str = "aevum.extensions") -> dict[str, type[Extension]]:
    """Load extensions from entry points."""
    extensions = {}
    try:
        for entry_point in importlib.metadata.entry_points(group=group):
            extensions[entry_point.name] = entry_point.load()
    except Exception:
        pass
    return extensions


# ========================================================================
# Configuration Management
# ========================================================================

class ConfigManager:
    """
    Manages configuration for the framework.

    Features:
    - Multiple config sources (files, env, dict)
    - Schema validation
    - Hot reload
    - Namespaced configs
    """

    def __init__(self):
        self._config: dict[str, Any] = {}
        self._schemas: dict[str, dict] = {}
        self._watchers: dict[str, list[Callable]] = {}

    def load_dict(self, config: dict[str, Any], namespace: str = "") -> None:
        """Load configuration from dictionary."""
        if namespace:
            self._config.setdefault(namespace, {}).update(config)
        else:
            self._config.update(config)

    def load_env(self, prefix: str = "AEVUM_") -> None:
        """Load configuration from environment variables."""
        for key, value in os.environ.items():
            if key.startswith(prefix):
                config_key = key[len(prefix):].lower().replace("__", ".")
                self._set_nested(config_key, value)

    def _set_nested(self, key: str, value: str) -> None:
        parts = key.split(".")
        target = self._config
        for part in parts[:-1]:
            target = target.setdefault(part, {})
        target[parts[-1]] = self._parse_value(value)

    def _parse_value(self, value: str) -> Any:
        if value.lower() in ("true", "false"):
            return value.lower() == "true"
        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            return value

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        parts = key.split(".")
        target = self._config
        for part in parts:
            if isinstance(target, dict):
                target = target.get(part)
            else:
                return default
            if target is None:
                return default
        return target

    def set(self, key: str, value: Any) -> None:
        """Set configuration value."""
        parts = key.split(".")
        target = self._config
        for part in parts[:-1]:
            target = target.setdefault(part, {})
        target[parts[-1]] = value
        self._notify_watchers(key, value)

    def get_namespace(self, namespace: str) -> dict[str, Any]:
        """Get all config for a namespace."""
        return self._config.get(namespace, {}).copy()

    def register_schema(self, namespace: str, schema: dict[str, Any]) -> None:
        """Register config schema for validation."""
        self._schemas[namespace] = schema

    def validate(self) -> list[str]:
        """Validate configuration against schemas."""
        errors = []
        for namespace, schema in self._schemas.items():
            config = self.get_namespace(namespace)
            for key, spec in schema.items():
                if spec.get("required", False) and key not in config:
                    errors.append(f"{namespace}.{key}: required but not set")
        return errors

    def watch(self, key: str, callback: Callable[[str, Any], None]) -> None:
        """Watch a config key for changes."""
        self._watchers.setdefault(key, []).append(callback)

    def _notify_watchers(self, key: str, value: Any) -> None:
        for callback in self._watchers.get(key, []):
            try:
                callback(key, value)
            except Exception:
                pass


_config_manager: ConfigManager | None = None


def get_config_manager() -> ConfigManager:
    """Get the global config manager."""
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager()
    return _config_manager


# ========================================================================
# Plugin System
# ========================================================================

class Plugin(ABC):
    """Base class for plugins."""

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def version(self) -> str:
        pass

    @abstractmethod
    async def setup(self, app: "Application") -> None:
        pass

    @abstractmethod
    async def teardown(self) -> None:
        pass


class Application:
    """
    Base application class for building frameworks.

    Provides:
    - Extension management
    - Configuration
    - Lifecycle
    - Plugin system
    """

    def __init__(
        self,
        name: str,
        *,
        loop: DebugEventLoop | None = None,
        config: LoopConfig | None = None,
    ):
        self.name = name
        self._loop = loop
        self._config = config or LoopConfig()
        self._extensions = ExtensionManager(self._loop or get_event_loop())
        self._config_manager = ConfigManager()
        self._plugins: dict[str, Plugin] = {}
        self._startup_hooks: list[Callable] = []
        self._shutdown_hooks: list[Callable] = []

    @property
    def loop(self) -> DebugEventLoop:
        if self._loop is None:
            self._loop = DebugEventLoop(self._config)
        return self._loop

    @property
    def config(self) -> ConfigManager:
        return self._config_manager

    @property
    def extensions(self) -> ExtensionManager:
        return self._extensions

    def add_startup_hook(self, hook: Callable) -> None:
        self._startup_hooks.append(hook)

    def add_shutdown_hook(self, hook: Callable) -> None:
        self._shutdown_hooks.append(hook)

    async def setup(self) -> None:
        """Setup the application."""
        # Load extensions from config
        ext_configs = self._config_manager.get_namespace("extensions")
        for name, ext_config in ext_configs.items():
            if isinstance(ext_config, dict) and "class" in ext_config:
                ext_class = self._import_class(ext_config["class"])
                self._extensions.load_extension(name, ext_class, ext_config.get("config"))

        await self._extensions.start_all()

        # Setup plugins
        for plugin in self._plugins.values():
            await plugin.setup(self)

        # Run startup hooks
        for hook in self._startup_hooks:
            if asyncio.iscoroutinefunction(hook):
                await hook()
            else:
                hook()

    async def run(self) -> None:
        """Run the application."""
        await self.setup()
        try:
            await self.loop.run_forever()
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        """Shutdown the application."""
        for hook in reversed(self._shutdown_hooks):
            if asyncio.iscoroutinefunction(hook):
                await hook()
            else:
                hook()

        for plugin in reversed(list(self._plugins.values())):
            await plugin.teardown()

        await self._extensions.stop_all()
        await self._extensions.shutdown_all()

        if self._loop:
            self._loop.close()

    def register_plugin(self, plugin: Plugin) -> None:
        if plugin.name in self._plugins:
            raise ValueError(f"Plugin '{plugin.name}' already registered")
        self._plugins[plugin.name] = plugin

    def _import_class(self, path: str) -> type:
        module_path, class_name = path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, class_name)


@asynccontextmanager
async def application_context(
    name: str,
    *,
    loop: DebugEventLoop | None = None,
    config: LoopConfig | None = None,
) -> AsyncIterator[Application]:
    """Context manager for application lifecycle."""
    app = Application(name, loop=loop, config=config)
    await app.setup()
    try:
        yield app
    finally:
        await app.shutdown()