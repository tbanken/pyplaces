"""I/O utility functions for reading data."""

from __future__ import annotations

import functools
import operator
from json import loads
import sys

from pyarrow import RecordBatchReader
from pyarrow.compute import field
from pyarrow.dataset import dataset
from pyarrow.fs import S3FileSystem
from geopandas import GeoDataFrame

from ._utils import build_filter_expression, catch_column_filter_error, FilterStructure
from ._geo_utils import geocode_place_to_bbox, geocode_point_to_bbox
from ._errors import S3ReadError

def decode_bytes(obj):
    """Recursively decode byte strings in nested structures."""
    if isinstance(obj, dict):
        return {decode_bytes(k): decode_bytes(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [decode_bytes(i) for i in obj]
    elif isinstance(obj, bytes):
        return obj.decode("utf-8")
    else:
        return obj

def read_geoparquet_arrow(path: str, region: str, bbox: tuple[float,float,float,float], 
                        columns: list[str] | None = None, 
                        filters: FilterStructure | None = None) -> GeoDataFrame:
    """
    Read geospatial data from a parquet file on S3 with filtering by bbox.
    
    Parameters:
    -----------
    path : str
        S3 path to the parquet file
    region : str
        AWS region
    bbox : tuple
        Bounding box as (minx, miny, maxx, maxy)
    columns : list, optional
        Columns to select
    filters : FilterStructure, optional
        Filter expression
        
    Returns:
    --------
    GeoDataFrame
        Filtered geodataframe
    """
    filter_expr = build_filter_expression(filters)
    
    xmin, ymin, xmax, ymax = bbox
    
    geo_filter_expr = (
        (field("bbox", "xmin") < xmax)
        & (field("bbox", "xmax") > xmin)
        & (field("bbox", "ymin") < ymax)
        & (field("bbox", "ymax") > ymin)
    )
    filter_ls = list(filter(lambda x: x is not None, [geo_filter_expr, filter_expr]))
    combined_filter = functools.reduce(operator.and_, filter_ls)
    
    clean_path = path.replace("s3://", "")
    try:
        ds = dataset(
            clean_path, filesystem=S3FileSystem(anonymous=True, region=region)
        )
    except Exception as e:
        raise S3ReadError(f"Read from bucket {clean_path} could not be complete.") from e
    
    try:
        if columns:
            batches = ds.to_batches(columns=columns, filter=combined_filter)
        else:
            batches = ds.to_batches(filter=combined_filter)
    except Exception as e:
        exc_info = sys.exc_info()[0]
        catch_column_filter_error(exc_info,e)
    
    non_empty_batches = (b for b in batches if b.num_rows > 0)
    
    schema = ds.schema
    metadata_str = decode_bytes(schema.metadata) 
    geo_dict = loads(metadata_str["geo"])
    geo_column = geo_dict["primary_column"]
    
    geometry_field_index = schema.get_field_index(geo_column)
    geometry_field = schema.field(geometry_field_index)
    geoarrow_geometry_field = geometry_field.with_metadata(
        {b"ARROW:extension:name": b"geoarrow.wkb"}
    )

    geoarrow_schema = schema.set(geometry_field_index, geoarrow_geometry_field)
    reader = RecordBatchReader.from_batches(geoarrow_schema, non_empty_batches)

    return GeoDataFrame.from_arrow(reader)

def read_parquet_arrow(path: str, region: str, 
                    columns: list[str] | None = None, 
                    filters: FilterStructure | None = None) -> GeoDataFrame:
    """
    Read tabular data from a parquet file on S3.
    
    Parameters:
    -----------
    path : str
        S3 path to the parquet file
    region : str
        AWS region
    columns : list, optional
        Columns to select
    filters : FilterStructure, optional
        Filter expression
        
    Returns:
    --------
    GeoDataFrame
        Filtered dataframe
    """
    filter_expr = build_filter_expression(filters)
    clean_path = path.replace("s3://", "")
    ds = dataset(
        clean_path, filesystem=S3FileSystem(anonymous=True, region=region)
    )
    try:
        if columns and filter_expr:
            batches = ds.to_batches(columns=columns, filter=filter_expr)
        elif filter_expr:
            batches = ds.to_batches(filter=filter_expr)
        else:
            batches = ds.to_batches(columns=columns)
    except Exception as e:
        exc_info = sys.exc_info()[0]
        catch_column_filter_error(exc_info,e)
    
    non_empty_batches = (b for b in batches if b.num_rows > 0)
    schema = ds.schema
    reader = RecordBatchReader.from_batches(schema, non_empty_batches)
    return reader.read_pandas()

def get_gdf_from_bbox(release:str, bbox:tuple[float,float,float,float], columns:list[str], filters: FilterStructure, prefix: str, path: str, region: str):
    """Helper function to get a geodataframe from a bounding box."""
    main_path = path.format(release=release) + prefix
    gdf = read_geoparquet_arrow(main_path, region, bbox, columns=columns, filters=filters)
    return gdf

def from_address(address: str | tuple[float,float], prefix: str, main_path: str, region: str,
            release: str, columns: list[str]| None = None, filters: FilterStructure| None = None,
            distance: float = 500, unit: str = "m") -> GeoDataFrame:
    """
    Wrapper to geocode an address and fetch the geoparquet data within the address's area.
    
    Parameters:
    -----------
    address : str or tuple
        Address string or (longitude, latitude) tuple
    prefix : str
        Path prefix for the data
    main_path : str
        Base path template with {release} placeholder
    region : str
        AWS region
    release : str
        Release version
    columns : list, optional
        Columns to select
    filters : FilterStructure, optional
        Filter expression
    distance : float, default 500
        Buffer distance
    unit : str, default 'm'
        Unit of distance
        
    Returns:
    --------
    GeoDataFrame
        Filtered geodataframe
    """
    bbox = geocode_point_to_bbox(address, distance, unit)
    gdf = from_bbox(bbox,prefix,main_path,region,release,columns,filters)
    return gdf
    
def from_place(address: str, prefix: str, main_path: str, region: str, release: str,
            columns: list[str]| None=None, filters: FilterStructure| None=None) -> GeoDataFrame:
    """
    Wrapper to geocode a place and fetch the geoparquet data within the place.
    
    Parameters:
    -----------
    address : str
        Place name or address
    prefix : str
        Path prefix for the data
    main_path : str
        Base path template with {release} placeholder
    region : str
        AWS region
    release : str
        Release version
    columns : list, optional
        Columns to select
    filters : FilterStructure, optional
        Filter expression
        
    Returns:
    --------
    GeoDataFrame
        Filtered geodataframe
    """
    geometry, bbox = geocode_place_to_bbox(address)
    gdf = from_bbox(bbox,prefix,main_path,region,release,columns,filters)
    filtered_gdf = gdf[gdf.within(geometry)]
    return filtered_gdf

def from_bbox(bbox: tuple[float,float,float,float], prefix: str, main_path: str, region: str, 
            release: str, columns: list[str]| None=None, filters: FilterStructure| None=None) -> GeoDataFrame:
    """
    Wrapper to fetch the geoparquet data within the bounding box.
    
    Parameters:
    -----------
    bbox : tuple
        Bounding box as (minx, miny, maxx, maxy)
    prefix : str
        Path prefix for the data
    main_path : str
        Base path template with {release} placeholder
    region : str
        AWS region
    release : str
        Release version
    columns : list, optional
        Columns to select
    filters : FilterStructure, optional
        Filter expression
        
    Returns:
    --------
    GeoDataFrame
        Filtered geodataframe
    """
    gdf = get_gdf_from_bbox(release, bbox, columns, filters, prefix, main_path, region)
    return gdf