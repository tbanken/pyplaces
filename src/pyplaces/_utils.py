"""Various utility functions."""

from __future__ import annotations

import sys
import functools
import inspect

def _run_before_decorator(before_func):
    """Returns a decorator that runs the specified `before_func` before the wrapped function."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Extract 'release' argument
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()

            if "release" in bound_args.arguments:
                release_value = bound_args.arguments["release"]
                before_func(release_value)

            return func(*args, **kwargs)

        return wrapper
    return decorator

def wrap_functions_with_release(module_name, before_func,func_list):
    """Dynamically wraps all functions with 'release' parameter in the given module."""
    module = sys.modules[module_name]
    
    for name in dir(module):
        attr = getattr(module, name)
        if callable(attr) and name != before_func.__name__ and name in func_list: # Wrap only user-facing functions with "release"
            sig = inspect.signature(attr)
            if "release" in sig.parameters:  
                setattr(module, name, _run_before_decorator(before_func)(attr))

