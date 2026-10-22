"""
Aevum Core - Enhanced Event Loop Foundation

This module provides the core event loop enhancements including:
- DebugEventLoop: Event loop with comprehensive debugging capabilities
- LoopPolicy: Custom event loop policy with configuration
- LoopContext: Context manager for loop lifecycle
- LoopMetrics: Built-in metrics collection
"""

from .loop import DebugEventLoop, LoopConfig, LoopMetrics, LoopState, new_event_loop, get_event_loop
from .policy import LoopPolicy, get_policy, set_policy, configure_default_loop
from .policy import LoopPolicy, get_policy, set_policy
from .context import LoopContext, current_loop, set_current_loop
from .exceptions import AevumError, LoopClosedError, LoopConfigurationError

__all__ = [
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
]

__version__ = "1.0.0"