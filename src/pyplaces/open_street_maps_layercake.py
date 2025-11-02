"""Functions to fetch geoparquet data from OSM Layercake datasets"""
from geopandas import GeoDataFrame
from pandas import DataFrame, json_normalize, concat
from ._io_utils import from_address, from_bbox, from_place, schema_from_dataset

# OSM Layercake dataset URLs
LAYERCAKE_BASE_URL = "https://data.openstreetmap.us/layercake/"
# OSM_DATASETS = {
#     "buildings": f"{LAYERCAKE_BASE_URL}buildings.parquet",
#     "highways": f"{LAYERCAKE_BASE_URL}highways.parquet"
# }

def layercake_buildings_from_address(address: str | tuple[float, float],
                                columns: list[str] | None = None,
                                filters: str | None = None,
                                distance: float = 500,
                                unit: str = "m") -> GeoDataFrame:
    """
    Retrieves OSM Layercake buildings data in a bounding box around a specified address.
    
    Parameters
    ----------
    address : str | tuple[float, float]
        The address or (longitude, latitude) tuple to search for nearby buildings.
    columns : list[str] | None, optional
        Specific columns to retrieve from the dataset.
    filters : str | None, optional
        DuckDB SQL expression for filtering results.
    distance : float, default 500
        Radius of the bounding box around the address. Defaults to 500 meters.
    unit : str, default "m"
        Unit of measurement for the distance. Defaults to "m" (meters). 
        One of: "m", "km", "in", "ft", "yd", "mi"
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing OSM buildings data within the specified bounding box.
    """
    gdf = from_address(address, "", LAYERCAKE_BASE_URL, None, 
                    None, columns, filters, distance, unit,"buildings")
    tags_df = json_normalize(gdf['tags'])
    gdf = concat([
        gdf.iloc[:, :2],           # First 2 columns
        tags_df,                    # Unpacked tags
        gdf.iloc[:, 2:].drop(columns=['tags'])  # Rest of columns (excluding tags)
    ], axis=1)
    return gdf


def layercake_buildings_from_place(address: str,
                            columns: list[str] | None = None,
                            filters: str | None = None) -> GeoDataFrame:
    """
    Retrieves OSM Layercake buildings data for a specific place identified by address or name.
    
    Parameters
    ----------
    address : str
        The address or identifier of the place to retrieve data for.
    columns : list[str] | None, optional
        Specific columns to retrieve from the dataset.
    filters : str | None, optional
        DuckDB SQL expression for filtering results.
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing OSM buildings data within specified place.
    """
    gdf = from_place(address, "", LAYERCAKE_BASE_URL, None, 
                    None, columns, filters,"buildings")
    tags_df = json_normalize(gdf['tags'])
    gdf = concat([
        gdf.iloc[:, :2],           # First 2 columns
        tags_df,                    # Unpacked tags
        gdf.iloc[:, 2:].drop(columns=['tags'])  # Rest of columns (excluding tags)
    ], axis=1)
    return gdf


def layercake_buildings_from_bbox(bbox: tuple[float, float, float, float],
                            columns: list[str] | None = None,
                            filters: str | None = None) -> GeoDataFrame:
    """
    Retrieves OSM Layercake buildings data within a specified bounding box.
    
    Parameters
    ----------
    bbox : tuple[float, float, float, float]
        Bounding box coordinates in the format (min_x, min_y, max_x, max_y).
    columns : list[str] | None, optional
        Specific columns to retrieve from the dataset.
    filters : str | None, optional
        DuckDB SQL expression for filtering results.
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing OSM buildings data within the specified bounding box.
    """
    gdf = from_bbox(bbox, "", LAYERCAKE_BASE_URL, None, 
                    None, columns, filters,"buildings")
    tags_df = json_normalize(gdf['tags'])
    gdf = concat([
        gdf.iloc[:, :2],           # First 2 columns
        tags_df,                    # Unpacked tags
        gdf.iloc[:, 2:].drop(columns=['tags'])  # Rest of columns (excluding tags)
    ], axis=1)
    return gdf


def layercake_highways_from_address(address: str | tuple[float, float],
                            columns: list[str] | None = None,
                            filters: str | None = None,
                            distance: float = 500,
                            unit: str = "m") -> GeoDataFrame:
    """
    Retrieves OSM Layercake highways data in a bounding box around a specified address.
    
    Parameters
    ----------
    address : str | tuple[float, float]
        The address or (longitude, latitude) tuple to search for nearby highways.
    columns : list[str] | None, optional
        Specific columns to retrieve from the dataset.
    filters : str | None, optional
        DuckDB SQL expression for filtering results.
    distance : float, default 500
        Radius of the bounding box around the address. Defaults to 500 meters.
    unit : str, default "m"
        Unit of measurement for the distance. Defaults to "m" (meters). 
        One of: "m", "km", "in", "ft", "yd", "mi"
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing OSM highways data within the specified bounding box.
    """
    gdf = from_address(address, "", LAYERCAKE_BASE_URL, None, 
                    None, columns, filters, distance, unit,"highways")
    tags_df = json_normalize(gdf['tags'])
    gdf = concat([
        gdf.iloc[:, :2],           # First 2 columns
        tags_df,                    # Unpacked tags
        gdf.iloc[:, 2:].drop(columns=['tags'])  # Rest of columns (excluding tags)
    ], axis=1)
    return gdf
    


def layercake_highways_from_place(address: str,
                            columns: list[str] | None = None,
                            filters: str | None = None) -> GeoDataFrame:
    """
    Retrieves OSM Layercake highways data for a specific place identified by address or name.
    
    Parameters
    ----------
    address : str
        The address or identifier of the place to retrieve data for.
    columns : list[str] | None, optional
        Specific columns to retrieve from the dataset.
    filters : str | None, optional
        DuckDB SQL expression for filtering results.
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing OSM highways data within specified place.
    """
    gdf = from_place(address, "", LAYERCAKE_BASE_URL, None, 
                    None, columns, filters,"highways")
    tags_df = json_normalize(gdf['tags'])
    gdf = concat([
        gdf.iloc[:, :2],           # First 2 columns
        tags_df,                    # Unpacked tags
        gdf.iloc[:, 2:].drop(columns=['tags'])  # Rest of columns (excluding tags)
    ], axis=1)
    return gdf


def layercake_highways_from_bbox(bbox: tuple[float, float, float, float],
                            columns: list[str] | None = None,
                            filters: str | None = None) -> GeoDataFrame:
    """
    Retrieves OSM Layercake highways data within a specified bounding box.
    
    Parameters
    ----------
    bbox : tuple[float, float, float, float]
        Bounding box coordinates in the format (min_x, min_y, max_x, max_y).
    columns : list[str] | None, optional
        Specific columns to retrieve from the dataset.
    filters : str | None, optional
        DuckDB SQL expression for filtering results.
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing OSM highways data within the specified bounding box.
    """
    gdf = from_bbox(bbox, "", LAYERCAKE_BASE_URL, None, 
                    None, columns, filters,"highways")
    tags_df = json_normalize(gdf['tags'])
    gdf = concat([
        gdf.iloc[:, :2],           # First 2 columns
        tags_df,                    # Unpacked tags
        gdf.iloc[:, 2:].drop(columns=['tags'])  # Rest of columns (excluding tags)
    ], axis=1)
    return gdf


def get_schema(dataset: str = "buildings") -> DataFrame:
    """
    Get DuckDB schema for the specified OSM Layercake dataset.

    Parameters
    ----------
    dataset : str, default "buildings"
        The dataset to get schema for. One of: "buildings", "highways"
        
    Returns
    -------
    DataFrame
        DataFrame with columns showing DuckDB types.
        
    Raises
    ------
    ValueError
        If the specified dataset is not valid.
    """
    valid_datasets = ["buildings", "highways"]
    if dataset not in valid_datasets:
        raise ValueError(f"Invalid dataset: {dataset}. Must be one of {valid_datasets}")
    
    path = LAYERCAKE_BASE_URL + dataset
    schema = schema_from_dataset(path, None)
    return schema


__all__ = [
    "layercake_buildings_from_address",
    "layercake_buildings_from_bbox", 
    "layercake_buildings_from_place",
    "layercake_highways_from_address",
    "layercake_highways_from_bbox",
    "layercake_highways_from_place",
    "get_schema"
]