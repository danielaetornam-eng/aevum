"""
Event Loop Policy

Custom event loop policy for Aevum that provides:
- Default configuration
- Loop creation with proper setup
- Thread-local loop management
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any

from .config import LoopConfig
from .loop import DebugEventLoop, new_event_loop

_loop_local = threading.local()


class LoopPolicy(asyncio.DefaultEventLoopPolicy):
    """
    Custom event loop policy for Aevum.

    This policy creates DebugEventLoop instances with configurable defaults.
    """

    def __init__(self, config: LoopConfig | None = None):
        super().__init__()
        self._config = config or LoopConfig()
        self._loop_factory = DebugEventLoop

    @property
    def config(self) -> LoopConfig:
        return self._config

    @config.setter
    def config(self, value: LoopConfig) -> None:
        self._config = value

    def new_event_loop(self) -> DebugEventLoop:
        """Create a new event loop with the configured settings."""
        loop = self._loop_factory(self._config)
        return loop

    def get_event_loop(self) -> DebugEventLoop:
        """Get the event loop for the current context."""
        try:
            return super().get_event_loop()
        except RuntimeError:
            # No loop set, create one
            loop = self.new_event_loop()
            self.set_event_loop(loop)
            return loop

    def set_event_loop(self, loop: DebugEventLoop | None) -> None:
        """Set the event loop for the current context."""
        if loop is not None and not isinstance(loop, DebugEventLoop):
            raise TypeError(f"Expected DebugEventLoop, got {type(loop).__name__}")
        super().set_event_loop(loop)

    def get_child_watcher(self) -> asyncio.AbstractChildWatcher:
        """Get the child watcher."""
        return super().get_child_watcher()

    def set_child_watcher(self, watcher: asyncio.AbstractChildWatcher) -> None:
        """Set the child watcher."""
        super().set_child_watcher(watcher)


_default_policy: LoopPolicy | None = None


def get_policy() -> LoopPolicy:
    """Get the current event loop policy."""
    global _default_policy
    if _default_policy is None:
        _default_policy = LoopPolicy()
    return _default_policy


def set_policy(policy: LoopPolicy) -> None:
    """Set the event loop policy."""
    global _default_policy
    if not isinstance(policy, LoopPolicy):
        raise TypeError(f"Expected LoopPolicy, got {type(policy).__name__}")
    _default_policy = policy
    asyncio.set_event_loop_policy(policy)


def configure_default_loop(config: LoopConfig | None = None) -> None:
    """Configure the default event loop policy."""
    policy = LoopPolicy(config)
    set_policy(policy)


def new_event_loop_with_policy(config: LoopConfig | None = None) -> DebugEventLoop:
    """Create a new event loop using the current policy."""
    return get_policy().new_event_loop()