"""Unit conversion utilities."""

from __future__ import annotations

def convert_to_meters(value: float, unit: str) -> float:
    """
    Convert a value from the specified unit to meters.
    
    Parameters:
    -----------
    value : float
        The value to convert
    unit : str
        The source unit ('m', 'km', 'in', 'ft', 'yd', 'mi')
        
    Returns:
    --------
    float
        Value in meters
    
    Raises:
    -------
    ValueError
        If the unit is not recognized
    """
    conversion_factors = {
        "m": 1,          # meters
        "km": 1000,      # kilometers to meters
        "in": 0.0254,    # inches to meters
        "ft": 0.3048,    # feet to meters
        "yd": 0.9144,    # yards to meters
        "mi": 1609.34,   # miles to meters
    }
    
    if unit not in conversion_factors:
        raise ValueError(f"Invalid unit: {unit}. Valid units: m, km, in, ft, yd, mi")
    
    return value * conversion_factors[unit]