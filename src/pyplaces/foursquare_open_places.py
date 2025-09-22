"""Functions to fetch geoparquet data from Foursquare Open Places on AWS"""
from importlib import resources
import requests
from io import StringIO
from typing import Union
from geopandas import GeoDataFrame
from pandas import DataFrame
from ._utils import wrap_functions_with_release
from ._io_utils import from_address, from_bbox, from_place, read_parquet_duckdb, schema_from_dataset
from ._category_finder import CategoryFinder

FSQ_VERSIONS_URL = "https://github.com/tbanken/pyplaces/blob/main/src/pyplaces/releases/foursquare/releases.txt"

def _check_or_get_release(release: str = None, url: str = FSQ_VERSIONS_URL ,latest: bool = False):
    """
    Validates if the specified Foursquare data release version exists.
    
    Parameters
    ----------
    release : str
        The release version to validate.
    
    url : str
        GitHub url to file version.
        
    Raises
    ------
    ValueError
        If the specified release version does not exist in the available releases.
    """
    raw_url = url.replace('github.com', 'raw.githubusercontent.com').replace('/blob/', '/')
    
    try:
        response = requests.get(raw_url,timeout=10)
        response.raise_for_status()
        file = StringIO(response.text)
        
        folders = [line.replace("dt=", "").strip(" \n/") for line in file]
    except requests.RequestException as e:
        print(f"Error fetching file: {e}, using local version check")
        if latest:
            print("Using local version file. Current release may not be up to date.")
        with resources.files("pyplaces").joinpath("releases/foursquare/releases.txt").open("r", encoding="utf-8-sig") as file:
            folders = [line.replace("dt=", "").strip(" \n/") for line in file]
    folders.remove("vector-tiles")
    if latest:
        return folders[-1]
    if release not in folders:
        raise ValueError(f"Invalid release: {release}")
    

FSQ_LATEST_RELEASE = _check_or_get_release(latest=True)

FSQ_MAIN_PATH = 's3://fsq-os-places-us-east-1/release/dt={release}/'
FSQ_BUCKET = 'fsq-os-places-us-east-1'
FSQ_REGION = 'us-east-1'
FSQ_PLACES_PREFIX = "places/parquet/"
FSQ_CATEGORIES_PREFIX = "categories/parquet/"
# FSQ_LATEST_RELEASE = "2025-08-07"

# FSQ_FUSED_MAIN_PATH = 's3://us-west-2.opendata.source.coop/fused/fsq-os-places/{release}/'
# FSQ_FUSED_BUCKET = 's-west-2.opendata.source.coop'
# FSQ_FUSED_REGION = 'us-west-2'
# FSQ_FUSED_LATEST_RELEASE = "2025-04-08"
# FSQ_FUSED_PLACES_PREFIX = "places/"

def foursquare_places_from_address(address: str | tuple[float,float],
                                    columns: list[str] | None = None,
                                    filters: str | None = None,
                                    distance: float = 500,
                                    unit: str = "m",
                                    release: str = FSQ_LATEST_RELEASE) -> GeoDataFrame:
    """
    Retrieves Foursquare places data in a bounding box around a specified address.
    
    Parameters
    ----------
    address : str | tuple[float,float]
        The addres or (longitude, latitude) tuple to search for nearby places.
    columns : list[str] | None, optional
        Specific columns to retrieve from the dataset.
    filters : str | None, optional
        DuckDB SQL expression
    distance : float, default 500
        Radius of the bounding box around the address. Defaults to 500 meters.
    unit : str, default "m"
        Unit of measurement for the distance. Defaults to "m" (meters). One of: "m","km","in","ft","yd","mi"
    release : str, default FSQ_LATEST_RELEASE
        Dataset release version to use. Defaults to the latest version.
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing Foursquare places data within the specified bounding box of the address.
    """
    return from_address(address, FSQ_PLACES_PREFIX, FSQ_MAIN_PATH, FSQ_REGION, release, columns, filters, distance, unit)


def foursquare_places_from_place(address: str,
                                    columns: list[str] | None = None,
                                    filters: str = None,
                                    release: str = FSQ_LATEST_RELEASE) -> GeoDataFrame:
    """
    Retrieves Foursquare places data for a specific place identified by its address or place name.
    
    Parameters
    ----------
    address : str
        The address or identifier of the place to retrieve data for.
    columns : list[str] | None, optional
        Specific columns to retrieve from the dataset.
    filters : str | None, optional
        DuckDB SQL expression
    release : str, default FSQ_LATEST_RELEASE
        Dataset release version to use. Defaults to the latest version.
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing Foursquare places data within specified place.
    """
    return from_place(address, FSQ_PLACES_PREFIX, FSQ_MAIN_PATH, FSQ_REGION, release, columns, filters)


def foursquare_places_from_bbox(bbox: tuple[float, float, float, float],
                                columns: list[str] | None = None,
                                filters: str | None = None,
                                release: str = FSQ_LATEST_RELEASE) -> GeoDataFrame:
    """
    Retrieves Foursquare places data within a specified bounding box.
    
    Parameters
    ----------
    bbox : tuple[float, float, float, float]
        Bounding box coordinates in the format (min_x, min_y, max_x, max_y).
    columns : list[str] | None, optional
        Specific columns to retrieve from the dataset. 
    filters : str | None, optional
        DuckDB SQL expression
    release : str, default FSQ_LATEST_RELEASE
        Dataset release version to use. Defaults to the latest version.
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing Foursquare places data within the specified bounding box.
    """
    return from_bbox(bbox, FSQ_PLACES_PREFIX, FSQ_MAIN_PATH, FSQ_REGION, release, columns, filters)


def get_categories(columns: list[str] | None = None,
                    filters: str | None = None,
                    release: str = FSQ_LATEST_RELEASE) -> DataFrame:
    """
    Retrieves Foursquare place categories data.
    
    Parameters
    ----------
    columns : list[str] | None, optional
        Specific columns to retrieve from the dataset.
    filters : str | None, optional
        DuckDB SQL expression
    release : str, default FSQ_LATEST_RELEASE
        Dataset release version to use. Defaults to the latest version.
        
    Returns
    -------
    DataFrame
        A DataFrame containing Foursquare place categories data.
        
    Notes
    -----
    This function constructs the data path using Foursquare-specific constants and
    retrieves the categories data using read_parquet_duckdb.
    """
    path = FSQ_MAIN_PATH.format(release=release) + FSQ_CATEGORIES_PREFIX
    return read_parquet_duckdb(path, FSQ_REGION, columns, filters)

def find_categories(search: str, num_results: int = 5, exact_match: bool=False,verbose: bool=False,as_df: bool= False) -> Union[list[str],DataFrame]:
    """
    Finds Foursquare Open Places categories based on a user search.
    
    Parameters
    ----------
    search : str
        User search term for matching.
    num_results : int, optional. Defaults to 5
        Number of matched categories to retrieve.
    exact_match : bool, optional. Defaults to False.
        Whether to retrieve only exact matches from search.
    verbose : bool, optional. Defaults to False.
        Whether to show the additional information of the matches
    as_df : bool, optional. Defaults to False.
        Whether to retrieve the matches as a DataFrame with additional information.
    
    Returns
    -------
    Union[list[str],Dataframe]
        Matched Foursquare Category IDs as a list of strings or DataFrame.
    """
    finder = CategoryFinder()
    categories = get_categories()
    finder.load_data(categories)
    finder.process_data()
    matches = finder.suggest_categories(search,num_results,exact_match,verbose,as_df,hide_ids=False,list_return="",show_name_and_id=True)
    return matches

def get_schema(categories=False,release:str=FSQ_LATEST_RELEASE) -> DataFrame:
    """
    Get DuckDB schema for the given dataset. Set categories to True if you want to get the categories instead of the places schema.

    Parameters
    ----------
        connector : bool, optional
            Whether to get categories schema, by default False.
        release : str, optional 
            Release version to use, defaults to the latest version.
    Returns
    -------
    DataFrame
        DataFrame with columns showing DuckDB types
    """
    path = FSQ_MAIN_PATH.format(release=release) 
    if categories:
        path = path + FSQ_CATEGORIES_PREFIX
    else:
        path = path + FSQ_PLACES_PREFIX
    schema = schema_from_dataset(path,FSQ_REGION)
    return schema

__all__ = ["foursquare_places_from_address", "foursquare_places_from_bbox", "foursquare_places_from_place", "get_categories","get_schema","find_categories"]

wrap_functions_with_release(__name__, _check_or_get_release,__all__)
