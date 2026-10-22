"""
Tests for Aevum Extensions
"""

import asyncio
import pytest

from aevum import (
    Extension,
    ExtensionMetadata,
    ExtensionManager,
    ConfigManager,
    Plugin,
    Application,
    application_context,
    ExtensionError,
    ExtensionNotFoundError,
)
from aevum.core import DebugEventLoop, LoopConfig


class TestExtensionMetadata:
    def test_metadata_creation(self):
        meta = ExtensionMetadata(
            name="test-ext",
            version="2.0.0",
            description="Test extension",
            author="Tester",
            dependencies=["dep1", "dep2"],
        )
        assert meta.name == "test-ext"
        assert meta.version == "2.0.0"
        assert meta.dependencies == ["dep1", "dep2"]


class TestExtension:
    @pytest.fixture
    def loop(self):
        loop = DebugEventLoop(LoopConfig())
        yield loop
        loop.close()

    @pytest.fixture
    def manager(self, loop):
        return ExtensionManager(loop)

    def test_extension_lifecycle(self, manager):
        events = []

        class TestExtension(Extension):
            metadata = ExtensionMetadata(name="lifecycle")

            def on_loop_init(self, loop):
                events.append("init")

            def on_loop_start(self):
                events.append("start")

            def on_loop_stop(self):
                events.append("stop")

            def on_loop_shutdown(self, loop):
                events.append("shutdown")

        ext = manager.load_extension("lifecycle", TestExtension)
        assert events == ["init"]

        asyncio.run(manager.start_all())
        assert "start" in events

        asyncio.run(manager.stop_all())
        assert "stop" in events

        asyncio.run(manager.shutdown_all())
        assert "shutdown" in events

    def test_extension_config(self, manager):
        class ConfigExtension(Extension):
            metadata = ExtensionMetadata(
                name="config-ext",
                config_schema={"api_key": {"required": True}}
            )

        ext = manager.load_extension("config-ext", ConfigExtension, {"api_key": "secret"})
        assert ext.config["api_key"] == "secret"

    def test_extension_missing_required_config(self, manager):
        class ConfigExtension(Extension):
            metadata = ExtensionMetadata(
                name="config-ext",
                config_schema={"api_key": {"required": True}}
            )

        with pytest.raises(ExtensionError):
            manager.load_extension("config-ext", ConfigExtension, {})

    def test_extension_dependencies(self, manager):
        class DepExtension(Extension):
            metadata = ExtensionMetadata(name="dep", dependencies=["missing"])

        with pytest.raises(ExtensionError, match="depends on 'missing'"):
            manager.load_extension("dep", DepExtension)

    def test_extension_unload(self, manager):
        class SimpleExt(Extension):
            metadata = ExtensionMetadata(name="simple")

        manager.load_extension("simple", SimpleExt)
        assert manager.has_extension("simple")

        manager.unload_extension("simple")
        assert not manager.has_extension("simple")

    def test_extension_unload_with_dependents(self, manager):
        class DepExt(Extension):
            metadata = ExtensionMetadata(name="dep-ext")

        class MainExt(Extension):
            metadata = ExtensionMetadata(name="main", dependencies=["dep-ext"])

        manager.load_extension("dep-ext", DepExt)
        manager.load_extension("main", MainExt)

        with pytest.raises(ExtensionError, match="required by 'main'"):
            manager.unload_extension("dep-ext")

    def test_get_extension(self, manager):
        class GetExt(Extension):
            metadata = ExtensionMetadata(name="get-ext")

        manager.load_extension("get-ext", GetExt)
        ext = manager.get_extension("get-ext")
        assert ext.name == "get-ext"

    def test_get_missing_extension(self, manager):
        with pytest.raises(ExtensionNotFoundError):
            manager.get_extension("missing")


class TestConfigManager:
    def test_load_dict(self):
        cm = ConfigManager()
        cm.load_dict({"key": "value"})
        assert cm.get("key") == "value"

    def test_load_dict_namespace(self):
        cm = ConfigManager()
        cm.load_dict({"nested": "value"}, namespace="app")
        assert cm.get("app.nested") == "value"

    def test_get_nested(self):
        cm = ConfigManager()
        cm.load_dict({"a": {"b": {"c": "deep"}}})
        assert cm.get("a.b.c") == "deep"

    def test_get_default(self):
        cm = ConfigManager()
        assert cm.get("missing", "default") == "default"

    def test_set_and_watch(self):
        cm = ConfigManager()
        changes = []

        def watcher(key, value):
            changes.append((key, value))

        cm.watch("watched", watcher)
        cm.set("watched", "new-value")
        assert changes == [("watched", "new-value")]

    def test_env_loading(self, monkeypatch):
        cm = ConfigManager()
        monkeypatch.setenv("AEVUM_TEST_KEY", "env-value")
        monkeypatch.setenv("AEVUM_NESTED__VALUE", "nested")

        cm.load_env("AEVUM_")
        assert cm.get("test_key") == "env-value"
        assert cm.get("nested.value") == "nested"

    def test_schema_validation(self):
        cm = ConfigManager()
        cm.register_schema("app", {"required_key": {"required": True}})

        errors = cm.validate()
        assert "app.required_key: required but not set" in errors

        cm.load_dict({"required_key": "value"}, namespace="app")
        errors = cm.validate()
        assert len(errors) == 0


class TestApplication:
    @pytest.mark.asyncio
    async def test_application_lifecycle(self):
        app = Application("test-app")

        started = False
        shutdown = False

        app.add_startup_hook(lambda: setattr(app, '_started', True))
        app.add_shutdown_hook(lambda: setattr(app, '_shutdown', True))

        await app.setup()
        assert hasattr(app, '_started')

        await app.shutdown()
        assert hasattr(app, '_shutdown')

    @pytest.mark.asyncio
    async def test_application_with_extension(self):
        events = []

        class AppExtension(Extension):
            metadata = ExtensionMetadata(name="app-ext")

            def on_loop_start(self):
                events.append("start")

        app = Application("test-app")
        app.config.load_dict({"class": "test_extensions.AppExtension"}, namespace="extensions")

        # Can't easily test without module, skip

    @pytest.mark.asyncio
    async def test_application_context_manager(self):
        async with application_context("ctx-app") as app:
            assert app.name == "ctx-app"
            assert app.loop is not None

        assert app.loop.is_closed


class TestPlugin:
    @pytest.mark.asyncio
    async def test_plugin_lifecycle(self):
        class TestPlugin(Plugin):
            name = "test-plugin"
            version = "1.0.0"

            def __init__(self):
                self.setup_called = False
                self.teardown_called = False

            async def setup(self, app):
                self.setup_called = True

            async def teardown(self):
                self.teardown_called = True

        plugin = TestPlugin()
        app = Application("test")
        app.register_plugin(plugin)

        await app.setup()
        assert plugin.setup_called

        await app.shutdown()
        assert plugin.teardown_called

    @pytest.mark.asyncio
    async def test_duplicate_plugin(self):
        class DupPlugin(Plugin):
            name = "dup"
            version = "1.0.0"

            async def setup(self, app):
                pass

            async def teardown(self):
                pass

        app = Application("test")
        app.register_plugin(DupPlugin())

        with pytest.raises(ValueError, match="already registered"):
            app.register_plugin(DupPlugin())