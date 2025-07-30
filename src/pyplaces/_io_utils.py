"""I/O utility functions for reading data."""

from __future__ import annotations
import json
from geopandas import GeoDataFrame
import duckdb
import geopandas as gpd


from ._geo_utils import geocode_place_to_bbox, geocode_point_to_bbox

def schema_from_dataset(s3_path,region):
    """
    Get schema from parquet dataset.
    
    Parameters:
    -----------
    s3_path : str 
        S3 path to directory containing parquet dataset
    region : str
        AWS region
    Returns:
    --------
    Schema
        DuckDB schema from given dataset.
    """
    conn = duckdb.connect()
    conn.execute(f"SET s3_region='{region}';")
    conn.execute("SET s3_use_ssl=true;")
    
    s3_path = s3_path + '*.parquet'
    schema_query = f"""
        SELECT DISTINCT name as 'Column' ,type as 'Type'
        FROM parquet_schema('{s3_path}');
        """
    df = conn.execute(schema_query).df()
    df = df.sort_values("Column").reset_index(drop="index")
    return df

def read_geoparquet_duckdb(path: str, region: str, bbox: tuple[float,float,float,float], 
                        columns: list[str] | None = None, 
                        filters: str | None = None) -> GeoDataFrame:
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
    filters : str, optional
        DuckDB SQL expression
        
    Returns:
    --------
    GeoDataFrame
        Filtered geodataframe
    """
    conn = duckdb.connect()
    
    # Install and load spatial extension for geometry handling
    conn.execute("INSTALL spatial;")
    conn.execute("LOAD spatial;")
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    conn.execute(f"SET s3_region='{region}';")
    conn.execute("SET s3_use_ssl=true;")
    
    xmin, ymin, xmax, ymax = bbox
    path = path + '*.parquet'
    
    metadata_query = f"""
                    SELECT decode(key) as 'key', decode(value) as 'value'
                    FROM parquet_kv_metadata('{path}')
                    WHERE key = 'geo' 
                    LIMIT 1;
                    """
    metadata_df = conn.execute(metadata_query).df()
    geom_col_name = json.loads(metadata_df['value'].iloc[0])['primary_column']
    
    # Select all if no selections, otherwise join selections together, exclude geometry column and cast it as text column
    column_select = ("*" if columns is None else ", ".join(columns)) + f" EXCLUDE ({geom_col_name}), ST_AsText({geom_col_name}) as 'geometry'"
    
    # Build spatial filter using bbox columns
    spatial_filter = f"""
        bbox.xmin < {xmax} AND
        bbox.xmax > {xmin} AND
        bbox.ymin < {ymax} AND
        bbox.ymax > {ymin}
    """
    
    # Construct the main query
    query = f"""
        SELECT {column_select}
        FROM read_parquet('{path}')
        WHERE {spatial_filter}
    """
    if filters:
        query = query + f" AND {filters}"
    try:
        df = conn.execute(query).df()
    except Exception as e:
        raise e
        # raise ValueError("Filter or Column error. Either the filter is not formatted properly, or the wrong column/value name is used.") from e
        
    
    conn.close()
    
    gdf = gpd.GeoDataFrame(df,crs="EPSG:4326",geometry=gpd.GeoSeries.from_wkt(df["geometry"]))  
    
    return gdf

def read_parquet_duckdb(path: str, region: str, 
                    columns: list[str] | None = None, 
                    filters: str | None = None) -> GeoDataFrame:
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
    filters : str, optional
        DuckDB SQL expression
        
    Returns:
    --------
    GeoDataFrame
        Filtered dataframe
    """
    conn = duckdb.connect()
    
    # Install and load spatial extension for geometry handling
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    conn.execute(f"SET s3_region='{region}';")
    conn.execute("SET s3_use_ssl=true;")
    
    path = path + '*.parquet'
    
    # Select all if no selections, otherwise join selections together, exclude geometry column and cast it as text column
    column_select = "*" if columns is None else ", ".join(columns)
    
    # Construct the main query
    query = f"""
        SELECT {column_select}
        FROM read_parquet('{path}')
    """
    if filters:
        query = query + f" AND {filters}"
        
    try:
        df = conn.execute(query).df()
    except Exception as e:
        # print(e)
        raise ValueError("Filter or Column error. Either the filter is not formatted properly, or the wrong column/value name is used.") from e
        
    conn.close()
    
    return df

def _get_gdf_from_bbox(release:str, bbox:tuple[float,float,float,float], columns:list[str], filters: str, prefix: str, path: str, region: str):
    """Helper function to get a geodataframe from a bounding box."""
    main_path = path.format(release=release) + prefix
    gdf = read_geoparquet_duckdb(main_path, region, bbox, columns=columns, filters=filters)
    return gdf

def from_address(address: str | tuple[float,float], prefix: str, main_path: str, region: str,
            release: str, columns: list[str]| None = None, filters: str | None = None,
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
    filters : str, optional
        DuckDB SQL expression
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
            columns: list[str]| None=None, filters: str| None=None) -> GeoDataFrame:
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
    filters : str, optional
        DuckDB SQL expression
        
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
            release: str, columns: list[str]| None=None, filters: str | None=None) -> GeoDataFrame:
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
    filters : str, optional
        DuckDB SQL expression
        
    Returns:
    --------
    GeoDataFrame
        Filtered geodataframe
    """
    gdf = _get_gdf_from_bbox(release, bbox, columns, filters, prefix, main_path, region)
    return gdf