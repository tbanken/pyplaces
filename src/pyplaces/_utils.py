"""Core utility functions."""

from __future__ import annotations

import sys
import functools
import inspect
import re
from typing import Union, List, Tuple, Any, Literal
from typing_extensions import TypeAlias

from pyarrow.compute import equal,not_equal,greater,less,greater_equal,less_equal,and_,or_
from pyarrow.types import is_struct
from pyarrow import array, scalar, RecordBatch

from ._errors import UnsupportedOperatorError

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

FieldName: TypeAlias = str
OperatorStr: TypeAlias = Literal["==", "!=", "<", "<=", ">", ">=","contains", "isin"]
FilterValue: TypeAlias = Union[str, int, float, List[Any], Tuple[Any, ...], None]
FilterTuple: TypeAlias = Tuple[FieldName, OperatorStr, FilterValue]
FilterGroup: TypeAlias = List[FilterTuple]

FilterStructure: TypeAlias = List[Union[FilterTuple, FilterGroup]] | FilterTuple
"""FilterStructure represents a list of filtering rules for a DataFrame-like object."""

    
def _parse_field_path(field_spec):
    """Parse a field specification that contains nested dictionary access."""
    path_parts = field_spec.split('.')
    base_column = path_parts[0]
    keys = path_parts[1:] if len(path_parts) > 1 else []
    
    return base_column, keys

def _extract_nested_value(array_column, keys):
    """Extract nested values from a dictionary/struct column."""
    current = array_column
    
    for key in keys:
        if is_struct(current.type):
            current = current.field(key)
        else:
            raise ValueError(f"Cannot access field '{key}' in non-struct column")
    
    return current

def _evaluate_condition(batch: RecordBatch, col: str, op:str, val:Any):
    """Return mask for batch based on filter."""
    if '.' in col:
        base_column, keys = _parse_field_path(col)
        # Extract the field value using the path
        if base_column in batch.column_names:
            col_values = _extract_nested_value(batch[base_column], keys)
        else:
            raise ValueError(f"Column '{base_column}' not found in dataset")
    else:
        col_values = batch[col]
    if op == "==":
        return equal(col_values, scalar(val))
    elif op == "!=":
        return not_equal(col_values, scalar(val))
    elif op == ">":
        return greater(col_values, scalar(val))
    elif op == "<":
        return less(col_values, scalar(val))
    elif op == ">=":
        return greater_equal(col_values, scalar(val))
    elif op == "<=":
        return less_equal(col_values, scalar(val))
    elif op == "contains":
        mask = [any(v in value for v in val) if value is not None and isinstance(val, list) else (val in value if value is not None else False) for value in col_values.to_pylist()]
        return array(mask)
    else:
        raise UnsupportedOperatorError(f"Unsupported operator: {op}")
    
def evaluate_filter_structure(batch: RecordBatch, structure):
    """
    Convert a place name to its geometry and bounding box.
    
    Parameters:
    -----------
    batch: PyArrow RecordBatch
        Batch to evaluate.
    structure: FilterStructure
        Filters used to evaluate.
        
        
    Filter structure rules:
    - A tuple (col, op, val) represents a single condition
    - Items at the same level are combined with OR
    - Items nested within a list are combined with AND
    
    Examples:
    - (col, op, val) - Simple condition
    - [tupleA, tupleB] - tupleA OR tupleB
    - [[tupleA, tupleB]] - tupleA AND tupleB  
    - [tupleA, [tupleB, tupleC]] - tupleA OR (tupleB AND tupleC)
    - [[tupleA, tupleB], [tupleC, tupleD]] - (tupleA AND tupleB) OR (tupleC AND tupleD)
        
    Returns:
    --------
    PyArrow Array
        PyArrow boolean array representing the combined filter mask
    """
    # Helper function for type checking
    def is_filter_triplet(item):
        return (isinstance(item, tuple) and len(item) == 3 and 
                isinstance(item[0], str) and isinstance(item[1], str))
    
    # Base case: single filter triplet
    if is_filter_triplet(structure):
        col, op, val = structure
        return _evaluate_condition(batch, col, op, val)
    
    # Must be a list
    if not isinstance(structure, list):
        raise ValueError(f"Invalid filter structure: {structure}")
    
    # All items at same level are OR'd together
    result_masks = []
    
    for item in structure:
        if is_filter_triplet(item):
            # Simple filter condition
            col, op, val = item
            mask = _evaluate_condition(batch, col, op, val)
            result_masks.append(mask)
        elif isinstance(item, list):
            # Nested list - items within are AND'd together
            and_masks = []
            for sub_item in item:
                sub_mask = evaluate_filter_structure(batch, sub_item)
                and_masks.append(sub_mask)
            
            if and_masks:
                # Combine with AND
                combined = and_masks[0]
                for mask in and_masks[1:]:
                    combined = and_(combined,mask)
                result_masks.append(combined)
        else:
            raise ValueError(f"Invalid filter element: {item}")
    
    # Combine all results at this level with OR
    if not result_masks:
        raise ValueError("No valid filters found in structure")
    
    result = result_masks[0]
    for mask in result_masks[1:]:
        result = or_(result,mask)
    
    return batch.filter(result)

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
        raise KeyError(f"Invalid column name:\"{name}\"") from error
    elif exc_type.__name__ =="ArrowNotImplementedError":
        match = re.search(r"\(([^)]+)\)", error_message)
        first_value,last_value = match.group(1).split(",")
        raise ValueError(f"Incorrect type used for value in filter: \"{last_value.strip()}\" should be \"{first_value.strip()}\"") from error