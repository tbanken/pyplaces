"""Core utility functions."""

from __future__ import annotations

import sys
import functools
import inspect
import re
from typing import Union, List, Tuple, Any, Literal
from typing_extensions import TypeAlias

from pyarrow.compute import field
from pyarrow.dataset import Expression

from ._errors import UnsupportedOperatorError, PyArrowError

def run_before_decorator(before_func):
    """Returns a decorator that runs the specified `before_func` before the wrapped function."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Extract 'release' argument dynamically
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()

            if "release" in bound_args.arguments:
                release_value = bound_args.arguments["release"]
                before_func(release_value)  # Call the module-specific before function

            return func(*args, **kwargs)

        return wrapper
    return decorator

def wrap_functions_with_release(module_name, before_func):
    """Dynamically wraps all functions with 'release' parameter in the given module."""
    module = sys.modules[module_name]
    
    for name in dir(module):
        attr = getattr(module, name)
        if callable(attr) and name != before_func.__name__:
            sig = inspect.signature(attr)
            if "release" in sig.parameters:  # Wrap only functions with "release"
                setattr(module, name, run_before_decorator(before_func)(attr))

FieldName: TypeAlias = str
OperatorStr: TypeAlias = Literal["==", "!=", "<", "<=", ">", ">=",
                                "is_nan", "is_null", "is_valid", "isin"]
FilterValue: TypeAlias = Union[str, int, float, List[Any], Tuple[Any, ...], None]
FilterTuple: TypeAlias = Tuple[FieldName, OperatorStr, FilterValue]
FilterGroup: TypeAlias = List[FilterTuple]

FilterStructure: TypeAlias = List[Union[FilterTuple, FilterGroup]] | FilterTuple
"""FilterStructure represents a list of filtering rules for a DataFrame-like object."""

def tuple_to_expression(filter_tuple: FilterTuple) -> Expression:
    """
    Convert a tuple of [field_name, operator, value] into a PyArrow expression.
    
    Parameters:
    -----------
    filter_tuple : tuple
        A tuple of (field_name, operator, value) where:
        - field_name (str): The name of the field to filter on
        - operator (str): The operator as a string ('>', '<', '==', etc.)
        - value: The value to compare against (str, int, or list)
    
    Returns:
    --------
    pyarrow.compute.Expression
        The resulting PyArrow expression
    """
    if len(filter_tuple) != 3:
        raise ValueError("Filter tuple must have exactly 3 elements: (field_name, operator, value)")
    
    field_name, op_str, value = filter_tuple
    pyaro_field = field(field_name)
    
    # Map operator strings to PyArrow operations    
    if op_str == "==" or op_str == "=":
        return pyaro_field == value
    elif op_str == "!=":
        return pyaro_field != value
    elif op_str == "<":
        return pyaro_field < value
    elif op_str == "<=":
        return pyaro_field <= value
    elif op_str == ">":
        return pyaro_field > value
    elif op_str == ">=":
        return pyaro_field >= value
    elif op_str == "is_nan":
        return pyaro_field.is_nan()
    elif op_str == "is_null":
        return pyaro_field.is_null()
    elif op_str == "is_valid":
        return pyaro_field.is_valid()
    elif op_str == "isin":
        if not isinstance(value, (list, tuple)):
            value = [value]
        return pyaro_field.isin(value)
    else:
        raise UnsupportedOperatorError(f"Unsupported operator: {op_str}")

def build_filter_expression(filter_structure: FilterStructure) -> Expression:
    """
    Build a PyArrow expression from a nested filter structure.
    
    Parameters:
    -----------
    filter_structure : list
        A list of filter groups, where each filter group is:
        - A single filter tuple (field_name, operator, value)
        - OR a list of filter tuples that should be combined with OR logic
    
    Returns:
    --------
    pyarrow.compute.Expression
        The resulting combined PyArrow expression
    """
    if not filter_structure:
        return None
    
    if not isinstance(filter_structure, list):
        filter_structure = [filter_structure]
    
    # Process each filter or filter group
    expressions = []
    
    for filter_item in filter_structure:
        if isinstance(filter_item, tuple):
            # Single filter tuple
            expressions.append(tuple_to_expression(filter_item))
        elif isinstance(filter_item, list):
            # List of OR'd filter tuples
            if not filter_item:
                continue  # Skip empty lists
                
            # Convert each tuple to expression
            or_expressions = [tuple_to_expression(ft) for ft in filter_item]
            
            # Combine with OR logic
            or_expr = or_expressions[0]
            for expr in or_expressions[1:]:
                or_expr = or_expr | expr  # Use '|' for logical OR
                
            expressions.append(or_expr)
    
    # Combine all expressions with AND logic
    combined_expr = expressions[0]
    for expr in expressions[1:]:
        combined_expr = combined_expr & expr  # Use '&' for logical AND
    
    return combined_expr

def catch_column_filter_error(exc_type: BaseException,error: Exception) -> None:
    """
    Throw user-friendly PyArrow errors.
    
    Parameters:
    -----------
    exc_type : BaseException
        Exception data
    error : Exception
        Exception thrown
    
    Returns:
    --------
    None
    """
    # Capture the full traceback
    error_message = str(error)
    # print(exc_type.__name__)
    if exc_type.__name__ == "UnsupportedOperatorError":
        raise error
    elif exc_type.__name__ == "ArrowInvalid":
        match = re.search(r"FieldRef\.Name\(([^)]+)\)", error_message)
        name = match.group(1)
        raise PyArrowError(f"Invalid column name:\"{name}\"") from error
    elif exc_type.__name__ =="ArrowNotImplementedError":
        match = re.search(r"\(([^)]+)\)", error_message)
        first_value,last_value = match.group(1).split(",")
        raise ValueError(f"Incorrect type used for value in filter: \"{last_value.strip()}\" should be \"{first_value.strip()}\"") from error