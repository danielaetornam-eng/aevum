"""
Shared context variables for aevum core.

This module breaks circular imports between loop.py and context.py.
"""

from __future__ import annotations

import contextvars
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .loop import DebugEventLoop

# Context variable for current loop
_loop_context_var: contextvars.ContextVar["DebugEventLoop | None"] = contextvars.ContextVar(
    "aevum_loop_context", default=None
)

# Context variable for signal handling
_handle_signals_var: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "aevum_handle_signals", default=True
)

def current_loop() -> "DebugEventLoop | None":
    """Get the current event loop from context."""
    return _loop_context_var.get(None)

def set_current_loop(loop: "DebugEventLoop | None") -> None:
    """Set the current event loop in context."""
    if loop is not None:
        _loop_context_var.set(loop)
    else:
        _loop_context_var.set(None)