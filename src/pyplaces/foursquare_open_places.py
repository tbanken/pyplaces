"""Functions to fetch  geoparquet data from Foursquare Open Places on AWS"""
from importlib import resources
from geopandas import GeoDataFrame
from pandas import DataFrame
from ._utils import FilterStructure, wrap_functions_with_release
from ._io_utils import from_address, from_bbox, from_place, read_parquet_arrow

#TODO latest release reads from text file
FSQ_MAIN_PATH = 's3://fsq-os-places-us-east-1/release/dt={release}/'
FSQ_BUCKET = 'fsq-os-places-us-east-1'
FSQ_REGION = 'us-east-1'
FSQ_PLACES_PREFIX = "places/parquet/"
FSQ_CATEGORIES_PREFIX = "categories/parquet/"
FSQ_LATEST_RELEASE = "2025-02-06"

FSQ_FUSED_MAIN_PATH = 's3://us-west-2.opendata.source.coop/fused/fsq-os-places/{release}/'
FSQ_FUSED_BUCKET = 's-west-2.opendata.source.coop'
FSQ_FUSED_REGION = 'us-west-2'
FSQ_FUSED_LATEST_RELEASE = "2025-01-10"
FSQ_FUSED_PLACES_PREFIX = "places/"

def foursquare_places_from_address(address: str, 
                                    columns: list[str] | None = None, 
                                    filters: FilterStructure | None = None, 
                                    distance: float = 500, 
                                    unit: str = "m", 
                                    release: str = FSQ_LATEST_RELEASE) -> GeoDataFrame:
    """
    Retrieves Foursquare places data around a specified address.
    
    Parameters
    ----------
    address : str
        The address to search for nearby places.
    columns : list[str] | None, optional
        Specific columns to retrieve from the dataset. If None, all columns are retrieved.
    filters : FilterStructure | None, optional
        Filter criteria to apply to the results.
    distance : float, default 500
        Radius of the search area around the address.
    unit : str, default "m"
        Unit of measurement for the distance (e.g., "m" for meters, "km" for kilometers).
    release : str, default FSQ_LATEST_RELEASE
        Foursquare data release version to use.
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing Foursquare places data within the specified distance of the address.
    """
    return from_address(address, FSQ_PLACES_PREFIX, FSQ_MAIN_PATH, FSQ_REGION, release, columns, filters, distance, unit)


def foursquare_places_from_place(address: str, 
                                    columns: list[str] | None = None, 
                                    filters: FilterStructure = None, 
                                    release: str = FSQ_LATEST_RELEASE) -> GeoDataFrame:
    """
    Retrieves Foursquare places data for a specific place identified by its address.
    
    Parameters
    ----------
    address : str
        The address or identifier of the place to retrieve data for.
    columns : list[str] | None, optional
        Specific columns to retrieve from the dataset. If None, all columns are retrieved.
    filters : FilterStructure, optional
        Filter criteria to apply to the results.
    release : str, default FSQ_LATEST_RELEASE
        Foursquare data release version to use.
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing Foursquare places data for the specified place.
    """
    return from_place(address, FSQ_PLACES_PREFIX, FSQ_MAIN_PATH, FSQ_REGION, release, columns, filters)


def foursquare_places_from_bbox(bbox: tuple[float, float, float, float], 
                                columns: list[str] | None = None, 
                                filters: FilterStructure | None = None, 
                                release: str = FSQ_LATEST_RELEASE) -> GeoDataFrame:
    """
    Retrieves Foursquare places data within a specified bounding box.
    
    Parameters
    ----------
    bbox : tuple[float, float, float, float]
        Bounding box coordinates in the format (min_x, min_y, max_x, max_y).
    columns : list[str] | None, optional
        Specific columns to retrieve from the dataset. If None, all columns are retrieved.
    filters : FilterStructure | None, optional
        Filter criteria to apply to the results.
    release : str, default FSQ_LATEST_RELEASE
        Foursquare data release version to use.
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing Foursquare places data within the specified bounding box.
    """
    return from_bbox(bbox, FSQ_PLACES_PREFIX, FSQ_MAIN_PATH, FSQ_REGION, release, columns, filters)


def get_categories(columns: list[str] | None = None, 
                    filters: FilterStructure | None = None, 
                    release: str = FSQ_LATEST_RELEASE) -> DataFrame:
    """
    Retrieves Foursquare place categories data.
    
    Parameters
    ----------
    columns : list[str] | None, optional
        Specific columns to retrieve from the dataset. If None, all columns are retrieved.
    filters : FilterStructure | None, optional
        Filter criteria to apply to the results.
    release : str, default FSQ_LATEST_RELEASE
        Foursquare data release version to use.
        
    Returns
    -------
    DataFrame
        A DataFrame containing Foursquare place categories data.
        
    Notes
    -----
    This function constructs the data path using Foursquare-specific constants and
    retrieves the categories data using read_parquet_arrow.
    """
    path = FSQ_MAIN_PATH.format(release=release) + FSQ_CATEGORIES_PREFIX
    return read_parquet_arrow(path, FSQ_REGION, columns, filters)


def _check_release(release: str):
    """
    Validates if the specified Foursquare data release version exists.
    
    Parameters
    ----------
    release : str
        The release version to validate.
        
    Raises
    ------
    ValueError
        If the specified release version does not exist in the available releases.
        
    Notes
    -----
    This private helper function reads available release versions from a text file
    and checks if the specified release is among them.
    """
    with resources.files("pyplaces").joinpath("releases/foursquare/releases.txt").open("r", encoding="utf-8-sig") as f:
        folders = [line.replace("dt=", "").strip(" \n/") for line in f]
    if release not in folders:
        raise ValueError(f"Invalid release: {release}")
    
wrap_functions_with_release(__name__, _check_release)
    
        # from pyarrow.parquet import ParquetFile
    # from pyarrow.fs import S3FileSystem
    # from rapidfuzz import process

    # # S3 Configuration
    # S3_BUCKET = "your-bucket-name"
    # S3_KEY = "path/to/categories.parquet"


    # def find_matching_foursquare_categories(user_input, s3_path, threshold=60, batch_size=1000):
    #     """Streams the Parquet file in chunks and performs fuzzy matching"""
    #     s3_filesystem = S3FileSystem()
    #     # Open the Parquet file
    #     parquet_file = ParquetFile(s3_path, filesystem=s3_filesystem)
        
    #     # Read in chunks
    #     for batch in parquet_file.iter_batches(batch_size, columns=["category_name", "category_id"]):
    #         df = batch.to_pandas()  # Convert batch to Pandas DataFrame
    #         categories_dict = dict(zip(df["category_name"], df["category_id"]))  # Convert to dict
            
    #         # Perform fuzzy matching
    #         matches = process.extract(user_input, categories_dict.keys(), limit=5, score_cutoff=threshold)
    #         if matches:
    #             return {name: categories_dict[name] for name, score, _ in matches}
        
    #     return {}  # Return empty dict if no matches

    # # Example usage
    # user_query = input("Enter category: ").strip()
    # s3_path = f"{S3_BUCKET}/{S3_KEY}"
    # matching_categories = find_matching_categories(user_query, s3_path, s3_filesystem)

    # if matching_categories:
    #     print("Matched Categories and IDs:", matching_categories)
    # else:
    #     print("No relevant categories found.")


__all__ = ["foursquare_places_from_address", "foursquare_places_from_bbox", "foursquare_places_from_place", "get_categories"]