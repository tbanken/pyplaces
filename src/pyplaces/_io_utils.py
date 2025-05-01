"""I/O utility functions for reading data."""

from __future__ import annotations
from json import loads
import sys

from pyarrow import RecordBatchReader
from pyarrow.compute import field
from pyarrow.dataset import dataset
from pyarrow.fs import S3FileSystem
from geopandas import GeoDataFrame

from ._utils import evaluate_filter_structure, catch_column_filter_error, FilterStructure
from ._geo_utils import geocode_place_to_bbox, geocode_point_to_bbox
from ._errors import S3ReadError

def schema_from_dataset(s3_path,region):
    """
    Get schema from PyArrow dataset.
    
    Parameters:
    -----------
    s3_path : str 
        S3 path to directory containing parquet dataset
    region : str
        AWS region
    Returns:
    --------
    Schema
        PyArrow schema from given dataset.
    """
    ds = dataset(
            s3_path, filesystem=S3FileSystem(anonymous=True, region=region)
        )
    return ds.schema

def _decode_bytes(obj):
    if isinstance(obj, dict):
        return {_decode_bytes(k): _decode_bytes(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_decode_bytes(i) for i in obj]
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
    # get pyarrow dataset
    clean_path = path.replace("s3://", "")
    try:
        ds = dataset(
            clean_path, filesystem=S3FileSystem(anonymous=True, region=region)
        )
    except Exception as e:
        raise S3ReadError(f"Read from bucket {clean_path} could not be complete.") from e
    
    xmin, ymin, xmax, ymax = bbox
    
    geo_filter_expr = (
        (field("bbox", "xmin") < xmax)
        & (field("bbox", "xmax") > xmin)
        & (field("bbox", "ymin") < ymax)
        & (field("bbox", "ymax") > ymin)
    )
    
    
    # filter with bounding box
    try:
        batches = ds.to_batches(filter=geo_filter_expr)
    except Exception as e:
        exc_info = sys.exc_info()[0]
        catch_column_filter_error(exc_info,e)
        
    non_empty_batches = (b for b in batches if b.num_rows > 0)
    
    
    
    def filter_batches(batches):
        for b in batches:
            yield evaluate_filter_structure(b,filters)
    
    # generate results from complex filters(if needed)
    if filters:
        filtered_batches = filter_batches(non_empty_batches)
    else:
        filtered_batches = non_empty_batches
    # convert geometry column to correct type in schema
    schema = ds.schema
    metadata_str = _decode_bytes(schema.metadata) 
    geo_dict = loads(metadata_str["geo"])
    geo_column = geo_dict["primary_column"]
    
    geometry_field_index = schema.get_field_index(geo_column)
    geometry_field = schema.field(geometry_field_index)
    geoarrow_geometry_field = geometry_field.with_metadata(
        {b"ARROW:extension:name": b"geoarrow.wkb"}
    )

    geoarrow_schema = schema.set(geometry_field_index, geoarrow_geometry_field)
    reader = RecordBatchReader.from_batches(geoarrow_schema, filtered_batches)
    gdf = GeoDataFrame.from_arrow(reader)
    gdf.set_crs("EPSG:4326",inplace=True)
    
    try:
        if columns:
            gdf = gdf[columns]
    except Exception as e:
        raise e
    
    return gdf

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
    
    # get pyarrow dataset
    clean_path = path.replace("s3://", "")
    try:
        ds = dataset(
            clean_path, filesystem=S3FileSystem(anonymous=True, region=region)
        )
    except Exception as e:
        raise S3ReadError(f"Read from bucket {clean_path} could not be complete.") from e
    
    try:
        batches = ds.to_batches()
    except Exception as e:
        exc_info = sys.exc_info()[0]
        catch_column_filter_error(exc_info,e)
        
    non_empty_batches = (b for b in batches if b.num_rows > 0)
    
    def filter_batches(batches):
        for b in batches:
            yield evaluate_filter_structure(b,filters)
    
    # generate results from complex filters(if needed)
    if filters:
        filtered_batches = filter_batches(non_empty_batches)
    else:
        filtered_batches = non_empty_batches
    
    schema = ds.schema
    reader = RecordBatchReader.from_batches(schema, filtered_batches)
    df = reader.read_pandas()
    if columns:
        df = df[columns]
    return df

def _get_gdf_from_bbox(release:str, bbox:tuple[float,float,float,float], columns:list[str], filters: FilterStructure, prefix: str, path: str, region: str):
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
    gdf = _get_gdf_from_bbox(release, bbox, columns, filters, prefix, main_path, region)
    return gdf