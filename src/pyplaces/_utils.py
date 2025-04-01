"""General utility functions."""

from __future__ import annotations

import sys
import operator
import functools
from typing import Union, List, Tuple, Any, Literal
from json import loads
import re
import numpy as np
from pyproj import Geod
from shapely import Polygon
from pyarrow import RecordBatchReader
from pyarrow.compute import field
from pyarrow.dataset import dataset, Expression
from pyarrow.fs import S3FileSystem
from geopandas import GeoDataFrame
from osmnx.geocoder import geocode, geocode_to_gdf
from osmnx import settings
from ._errors import S3ReadError, PyArrowError, UnsupportedOperatorError

#TODO add releases automation
#TODO add base types automation and code check



# def citation(style: str = "bibtex") -> None:
#     """
#     Print the OSMnx package's citation information.

#     Boeing, G. (2024). Modeling and Analyzing Urban Networks and Amenities with
#     OSMnx. Working paper. https://geoffboeing.com/publications/osmnx-paper/

#     Parameters
#     ----------
#     style
#         {"apa", "bibtex", "ieee"}
#         The citation format, either APA or BibTeX or IEEE.
#     """
#     if style == "apa":
#         msg = (
#             "Boeing, G. (2024). Modeling and Analyzing Urban Networks and Amenities "
#             "with OSMnx. Working paper. https://geoffboeing.com/publications/osmnx-paper/"
#         )
#     elif style == "bibtex":
#         msg = (
#             "@techreport{boeing_osmnx_2024,\n"
#             "    author = {Boeing, Geoff},\n"
#             "    title = {{Modeling and Analyzing Urban Networks and Amenities with OSMnx}},\n"
#             "    type = {Working paper},\n"
#             "    url = {https://geoffboeing.com/publications/osmnx-paper/},\n"
#             "    year = {2024}\n"
#             "}"
#         )
#     elif style == "ieee":
#         msg = (
#             'G. Boeing, "Modeling and Analyzing Urban Networks and Amenities with OSMnx," '
#             "Working paper, https://geoffboeing.com/publications/osmnx-paper/"
#         )
#     else:  # pragma: no cover
#         err_msg = f"Invalid citation style {style!r}."
#         raise ValueError(err_msg)

#     print(msg)  # noqa: T201


# def ts(style: str = "datetime", template: str | None = None) -> str:
#     """
#     Return current local timestamp as a string.

#     Parameters
#     ----------
#     style
#         {"datetime", "iso8601", "date", "time"}
#         Format the timestamp with this built-in style.
#     template
#         If not None, format the timestamp with this format string instead of
#         one of the built-in styles.

#     Returns
#     -------
#     timestamp
#         The current timestamp.
#     """
#     if template is None:
#         if style == "datetime":
#             template = "{:%Y-%m-%d %H:%M:%S}"
#         elif style == "iso8601":
#             template = "{:%Y-%m-%dT%H:%M:%SZ}"
#         elif style == "date":
#             template = "{:%Y-%m-%d}"
#         elif style == "time":
#             template = "{:%H:%M:%S}"
#         else:  # pragma: no cover
#             msg = f"Invalid timestamp style {style!r}."
#             raise ValueError(msg)

#     return template.format(dt.datetime.now().astimezone())

settings.http_referer = "pyplaces Python Package" # OSMnx Python package (https://github.com/gboeing/osmnx)
settings.http_user_agent= "pyplaces"

FieldName = str
OperatorStr = Literal["==", "!=", "<", "<=", ">", ">=", "is_nan", "is_null", "is_valid", "isin"]
FilterValue = Union[str, int, float, List[Any], Tuple[Any, ...], None]
FilterTuple = Tuple[FieldName, OperatorStr, FilterValue]
FilterGroup = List[FilterTuple]
FilterStructure = List[Union[FilterTuple, FilterGroup]] | FilterTuple

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
    
    Example:
    --------
    [
        ('age', '>', 30),                        # Single filter
        [('status', '==', 'active'),             # OR group
        ('status', '==', 'pending')],
        ('name', 'isin', ['John', 'Jane'])       # Single filter
    ]
    
    This creates: (age > 30) AND (status == 'active' OR status == 'pending') AND (name IN ['John', 'Jane'])
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


def catch_column_filter_error(func):
    try:
        return func
    except Exception as original_error:
        # Capture the full traceback
        exc_type= sys.exc_info()[0]
        
        error_message = str(original_error)
        # print(exc_type.__name__)
        if exc_type.__name__ == "UnsupportedOperatorError":
            raise original_error
        elif exc_type.__name__ == "ArrowInvalid":
            match = re.search(r"FieldRef\.Name\(([^)]+)\)", error_message)
            name = match.group(1)
            raise PyArrowError(f"Invalid column name:\"{name}\"") from original_error
        elif exc_type.__name__ =="ArrowNotImplementedError":
            match = re.search(r"\(([^)]+)\)", error_message)
            first_value,last_value = match.group(1).split(",")
            raise ValueError(f"Incorrect type used for value in filter: \"{last_value.strip()}\" should be \"{first_value.strip()}\"") from original_error  


def read_geoparquet_arrow(path: str,region: str,bbox: tuple[float,float,float,float],columns: list[str] | None = None,filters: FilterStructure | None = None) -> GeoDataFrame:
    filter_expr = build_filter_expression(filters)
    def decode_bytes(obj):
        if isinstance(obj, dict):
            return {decode_bytes(k): decode_bytes(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [decode_bytes(i) for i in obj]
        elif isinstance(obj, bytes):
            return obj.decode("utf-8")
        else:
            return obj
    
    xmin, ymin, xmax, ymax = bbox
    
    geo_filter_expr = (
        (field("bbox", "xmin") < xmax)
        & (field("bbox", "xmax") > xmin)
        & (field("bbox", "ymin") < ymax)
        & (field("bbox", "ymax") > ymin)
    )
    filter_ls = list(filter(lambda x: x is not None, [geo_filter_expr, filter_expr]))
    combined_filter = functools.reduce(operator.and_, filter_ls)
    
    clean_path=path.replace("s3://", "")
    try:
        ds = dataset(
        clean_path, filesystem=S3FileSystem(anonymous=True, region=region)
        )
    except Exception as e:
        raise S3ReadError(f"Read from bucket {clean_path} could not be complete.") from e
    if columns:
        batches = catch_column_filter_error(ds.to_batches(columns=columns,filter=combined_filter))
    else:
        batches = catch_column_filter_error(ds.to_batches(filter=combined_filter))
    #wrap this for incorrect field for incorrect column in filter or select, or value in filter
    
    non_empty_batches = (b for b in batches if b.num_rows > 0)
    
    schema = ds.schema
    metadata_str = decode_bytes(schema.metadata)  
    geo_dict = loads(metadata_str["geo"])
    geo_column =geo_dict["primary_column"]
    
    geometry_field_index = schema.get_field_index(geo_column)
    geometry_field = schema.field(geometry_field_index)
    geoarrow_geometry_field = geometry_field.with_metadata(
        {b"ARROW:extension:name": b"geoarrow.wkb"}
    )

    geoarrow_schema = schema.set(geometry_field_index, geoarrow_geometry_field)


    reader = RecordBatchReader.from_batches(geoarrow_schema, non_empty_batches)

    return GeoDataFrame.from_arrow(reader)

def read_parquet_arrow(path: str,region: str,columns: list[str] | None = None,filters: FilterStructure | None = None) -> GeoDataFrame:
    filter_expr = build_filter_expression(filters)
    clean_path=path.replace("s3://", "")
    ds = dataset(
        clean_path, filesystem=S3FileSystem(anonymous=True, region=region)
    )
    if columns and filter_expr:
        batches = catch_column_filter_error(ds.to_batches(columns=columns,filter=filter_expr))
    elif filter_expr:
        batches = catch_column_filter_error(ds.to_batches(filter=filter_expr))
    else:
        batches = catch_column_filter_error(ds.to_batches(columns=columns))

    non_empty_batches = (b for b in batches if b.num_rows > 0)
    schema = ds.schema
    reader = RecordBatchReader.from_batches(schema, non_empty_batches)

    return GeoDataFrame.from_arrow(reader)

def get_gdf_from_bbox(release,bbox,columns,filters,prefix,path,region):
    main_path = path.format(release=release) + prefix
    gdf = read_geoparquet_arrow(main_path,region,bbox,columns=columns,filters=filters)
    return gdf
    
def geocode_point_to_bbox(address,distance,unit):
    if isinstance(address,str):
        point = geocode(address)
    else:
        point = address
    distance = convert_to_meters(distance,unit)
    bbox = point_buffer(point[1],point[0],distance).bounds
    return bbox

def geocode_place_to_bbox(address):
    gdf = geocode_to_gdf(query=address,which_result=1,by_osmid=False)
    row = gdf.iloc[0]
    geometry = row["geometry"]
    bbox = (row["bbox_west"],row["bbox_south"],row["bbox_east"],row["bbox_north"])
    return geometry, bbox

def from_address(address: str | tuple[float,float],prefix: str,main_path: str, region: str,release: str,columns: list[str]| None = None,filters: FilterStructure| None = None,distance: float = 500 ,unit: str = "m")-> GeoDataFrame:
    bbox = geocode_point_to_bbox(address,distance,unit)
    gdf = get_gdf_from_bbox(release,bbox,columns,filters,prefix,main_path,region)
    return gdf
    
def from_place(address: str,prefix: str,main_path: str, region: str,release: str,columns: list[str]| None=None,filters: FilterStructure| None=None)-> GeoDataFrame:
    geometry,bbox = geocode_place_to_bbox(address)
    gdf = get_gdf_from_bbox(release,bbox,columns,filters,prefix,main_path,region)
    filtered_gdf = gdf[gdf.within(geometry)]
    return filtered_gdf

def from_bbox(bbox: tuple[float,float,float,float],prefix: str,main_path: str, region: str,release: str,columns: list[str]| None=None,filters: FilterStructure| None=None)-> GeoDataFrame:
    gdf = get_gdf_from_bbox(release,bbox,columns,filters,prefix,main_path,region)
    return gdf


def convert_to_meters(value: float,unit: str) -> float:
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



def point_buffer(lon: float, lat: float, radius_m:float) -> Polygon:
    # Use this instead of `.buffer()` provided by geodataframe
    # Adapted from:
    # https://stackoverflow.com/questions/31492220/how-to-plot-a-tissot-with-cartopy-and-matplotlib
    geod = Geod(ellps='WGS84')
    num_vtxs = 64
    lons, lats, _ = geod.fwd(np.repeat(lon, num_vtxs),
                                    np.repeat(lat, num_vtxs),
                                    np.linspace(360, 0, num_vtxs),
                                    np.repeat(radius_m, num_vtxs),
                                    radians=False
                            )
    return Polygon(zip(lons, lats))