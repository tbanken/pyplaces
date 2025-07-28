"""Functions to fetch  geoparquet data from Overture Maps on AWS"""
from importlib import resources
from uuid import uuid4
from typing import Union
from geopandas import GeoDataFrame
from pandas import read_csv, DataFrame
from ._utils import FilterStructure, wrap_functions_with_release
from ._io_utils import from_address, from_bbox, from_place, schema_from_dataset
from ._category_finder import CategoryFinder


#TODO latest release reads from text file
OVERTURE_MAIN_PATH = 's3://overturemaps-us-west-2/release/{release}/'
OVERTURE_BUCKET = 'overturemaps-us-west-2'
OVERTURE_REGION = 'us-west-2'
OVERTURE_LATEST_RELEASE = "2025-07-23.0"

OVERTURE_PLACES_PREFIX = "theme=places/type=place/"
OVERTURE_BUILDINGS_PREFIX = "theme=buildings/type=building/"
OVERTURE_BUILDINGS_PART_PREFIX = "theme=buildings/type=building_part/"
OVERTURE_ADDRESSES_PREFIX = "theme=addresses/type=address/"
OVERTURE_TRANSPORTATION_SEGMENT_PREFIX = "theme=transportation/type=segment/"
OVERTURE_TRANSPORTATION_CONNECTOR_PREFIX = "theme=transportation/type=connector/"
OVERTURE_BASE_PREFIX = "theme=base/type={type}"

OVERTURE_CATEGORIES_URL = "https://raw.githubusercontent.com/OvertureMaps/schema/refs/heads/main/docs/schema/concepts/by-theme/places/overture_categories.csv"

def overture_places_from_address(address: str | tuple[float,float],
                                columns: list[str]| None = None,
                                filters: FilterStructure | None = None,
                                distance: float = 500 ,
                                unit: str = "m" ,
                                release: str = OVERTURE_LATEST_RELEASE) -> GeoDataFrame:
    """
    Retrieve places data from Overture in a bounding box around a specified address.
    
    Parameters
    ----------
    address : str | tuple[float,float]
        The address or (longitude, latitude) tuple to search for nearby places.
    columns : list[str] | None, optional
        Specific columns to include in the result.
    filters : FilterStructure | None, optional
        Filter criteria to apply to the results.
        Should be a list in the format(column,operator,value)
        Supported operators are: "==", "!=", "<", "<=", ">", ">=","contains"
    distance : float, optional
        Radius of the bounding box around the address. Defaults to 500 meters.
    unit : str, optional
        Unit of measurement for the distance. Defaults to "m" (meters). One of: "m","km","in","ft","yd","mi"
    release : str, optional
        Dataset release version to use. Defaults to the latest version.
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing places data within the specified bounding box of the address
    """
    return from_address(address,OVERTURE_PLACES_PREFIX,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters,distance,unit)

def overture_places_from_place(address: str,columns: list[str]| None=None,filters: FilterStructure| None=None,release: str=OVERTURE_LATEST_RELEASE)-> GeoDataFrame:
    """
    Retrieve places data from Overture by its address or place name.
    
    Parameters
    ----------
    address : str
        The place name to search within
    columns : list[str] | None, optional
        Specific columns to include in the result.
    filters : FilterStructure | None, optional
        Filter criteria to apply to the results.
        Should be a list in the format(column,operator,value)
        Supported operators are: "==", "!=", "<", "<=", ">", ">=","contains"
    release : str, optional
        Dataset release version to use. Defaults to the latest version.
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing places data within the specified place
    """
    return from_place(address,OVERTURE_PLACES_PREFIX,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters)

def overture_places_from_bbox(bbox: tuple[float,float,float,float],columns: list[str]| None=None,filters: FilterStructure| None=None,release: str=OVERTURE_LATEST_RELEASE)-> GeoDataFrame:
    """
    Retrieve places data from Overture within a bounding box.
    
    Parameters
    ----------
    bbox : tuple[float, float, float, float]
        The bounding box coordinates (min_x, min_y, max_x, max_y)
    columns : list[str] | None, optional
        Specific columns to include in the result.
    filters : FilterStructure | None, optional
        Filter criteria to apply to the results.
        Should be a list in the format(column,operator,value)
        Supported operators are: "==", "!=", "<", "<=", ">", ">=","contains"
    release : str, optional
        Dataset release version to use. Defaults to the latest version.
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing places data within the specified bounding box
    """
    return from_bbox(bbox,OVERTURE_PLACES_PREFIX,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters)

def overture_buildings_from_address(address: str | tuple[float,float],columns: list[str]| None = None,filters: FilterStructure| None = None,distance: float = 500 ,unit: str = "m" ,release: str = OVERTURE_LATEST_RELEASE,building_part: bool=False) -> GeoDataFrame:
    """
    Retrieve buildings data from Overture in a bounding box around a specified address.
    
    Parameters
    ----------
    address : str | tuple[float,float]
        The address or (longitude, latitude) tuple to search for nearby places.
    columns : list[str] | None, optional
        Specific columns to include in the result.
    filters : FilterStructure | None, optional
        Filter criteria to apply to the results.
        Should be a list in the format(column,operator,value)
        Supported operators are: "==", "!=", "<", "<=", ">", ">=","contains"
    distance : float, optional
        Radius of the bounding box around the address. Defaults to 500 meters.
    unit : str, optional
        Unit of measurement for the distance. Defaults to "m" (meters). One of: "m","km","in","ft","yd","mi"
    release : str, optional
        Dataset release version to use. Defaults to the latest version.
    building_part : bool, optional
        Whether to retrieve building parts instead of whole buildings, by default False
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing buildings data within the specified bounding box of the address
    """
    if building_part:
        prefix = OVERTURE_BUILDINGS_PART_PREFIX
    else:
        prefix = OVERTURE_BUILDINGS_PREFIX 
    return from_address(address,prefix,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters,distance,unit)

def overture_buildings_from_place(address: str,columns: list[str]| None=None,filters: FilterStructure| None=None,release: str=OVERTURE_LATEST_RELEASE,building_part: bool=False)-> GeoDataFrame:
    """
    Retrieve buildings data from Overture by its address or place name.
    
    Parameters
    ----------
    address : str
        The place name to search within
    columns : list[str] | None, optional
        Specific columns to include in the result.
    filters : FilterStructure | None, optional
        Filter criteria to apply to the results.
        Should be a list in the format(column,operator,value)
        Supported operators are: "==", "!=", "<", "<=", ">", ">=","contains"
    release : str, optional
        Dataset release version to use. Defaults to the latest version.
    building_part : bool, optional
        Whether to retrieve building parts instead of whole buildings, by default False
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing buildings data within the specified place
    """
    if building_part:
        prefix = OVERTURE_BUILDINGS_PART_PREFIX
    else:
        prefix = OVERTURE_BUILDINGS_PREFIX
    return from_place(address,prefix,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters)

def overture_buildings_from_bbox(bbox: tuple[float,float,float,float],columns: list[str]| None=None,filters: FilterStructure| None=None,release: str=OVERTURE_LATEST_RELEASE,building_part: bool=False)-> GeoDataFrame:
    """
    Retrieve buildings data from Overture within a bounding box.
    
    Parameters
    ----------
    bbox : tuple[float, float, float, float]
        The bounding box coordinates (min_x, min_y, max_x, max_y)
    columns : list[str] | None, optional
        Specific columns to include in the result.
    filters : FilterStructure | None, optional
        Filter criteria to apply to the results.
        Should be a list in the format(column,operator,value)
        Supported operators are: "==", "!=", "<", "<=", ">", ">=","contains"
    release : str, optional
        Dataset release version to use. Defaults to the latest version.
    building_part : bool, optional
        Whether to retrieve building parts instead of whole buildings, by default False
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing buildings data within the specified bounding box
    """
    if building_part:
        prefix = OVERTURE_BUILDINGS_PART_PREFIX
    else:
        prefix = OVERTURE_BUILDINGS_PREFIX
    return from_bbox(bbox,prefix,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters)

def overture_transportation_from_address(address: str | tuple[float,float],columns: list[str]| None = None,filters: FilterStructure| None = None,distance: float = 500 ,unit: str = "m" ,release: str = OVERTURE_LATEST_RELEASE,connector:bool=False) -> GeoDataFrame:
    """
    Retrieve transportation data from Overture in a bounding box around a specified address.
    
    Parameters
    ----------
    address : str | tuple[float,float]
        The address or (longitude, latitude) tuple to search for nearby places.
    columns : list[str] | None, optional
        Specific columns to include in the result.
    filters : FilterStructure | None, optional
        Filter criteria to apply to the results.
        Should be a list in the format(column,operator,value)
        Supported operators are: "==", "!=", "<", "<=", ">", ">=","contains"
    distance : float, optional
        Radius of the bounding box around the address. Defaults to 500 meters.
    unit : str, optional
        Unit of measurement for the distance. Defaults to "m" (meters). One of: "m","km","in","ft","yd","mi"
    release : str, optional
        Dataset release version to use. Defaults to the latest version.
    connector : bool, optional
        Whether to retrieve connectors instead of segments, by default False
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing transportation data within the specified bounding box of the address
    """
    if connector:
        prefix = OVERTURE_TRANSPORTATION_CONNECTOR_PREFIX
    else:
        prefix = OVERTURE_TRANSPORTATION_SEGMENT_PREFIX
    return from_address(address,prefix,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters,distance,unit)

def overture_transportation_from_place(address: str,columns: list[str]| None=None,filters: FilterStructure| None=None,release: str=OVERTURE_LATEST_RELEASE,connector:bool=False)-> GeoDataFrame:
    """
    Retrieve transportation data from Overture by its address or place name.
    
    Parameters
    ----------
    address : str
        The place name to search within
    columns : list[str] | None, optional
        Specific columns to include in the result.
    filters : FilterStructure | None, optional
        Filter criteria to apply to the results.
        Should be a list in the format(column,operator,value)
        Supported operators are: "==", "!=", "<", "<=", ">", ">=","contains"
    release : str, optional
        Dataset release version to use. Defaults to the latest version.
    connector : bool, optional
        Whether to retrieve connectors instead of segments, by default False
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing transportation data within the specified place
    """
    if connector:
        prefix = OVERTURE_TRANSPORTATION_CONNECTOR_PREFIX
    else:
        prefix = OVERTURE_TRANSPORTATION_SEGMENT_PREFIX
    return from_place(address,prefix,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters)

def overture_transportation_from_bbox(bbox: tuple[float,float,float,float],columns: list[str]| None=None,filters: FilterStructure | None=None,release: str=OVERTURE_LATEST_RELEASE,connector:bool=False)-> GeoDataFrame:
    """
    Retrieve transportation data from Overture within a bounding box.
    
    Parameters
    ----------
    bbox : tuple[float, float, float, float]
        The bounding box coordinates (min_x, min_y, max_x, max_y)
    columns : list[str] | None, optional
        Specific columns to include in the result.
    filters : FilterStructure | None, optional
        Filter criteria to apply to the results.
        Should be a list in the format(column,operator,value)
        Supported operators are: "==", "!=", "<", "<=", ">", ">=","contains"
    release : str, optional
        Dataset release version to use. Defaults to the latest version.
    connector : bool, optional
        Whether to retrieve connectors instead of segments, by default False
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing transportation data within the specified bounding box
    """
    if connector:
        prefix = OVERTURE_TRANSPORTATION_CONNECTOR_PREFIX
    else:
        prefix = OVERTURE_TRANSPORTATION_SEGMENT_PREFIX
    return from_bbox(bbox,prefix,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters)
    
def overture_addresses_from_address(address: str | tuple[float,float],columns: list[str]| None = None,filters: FilterStructure| None = None,distance: float = 500 ,unit: str = "m" ,release: str = OVERTURE_LATEST_RELEASE) -> GeoDataFrame:
    """
    Retrieve address data from Overture in a bounding box around a specified address.
    
    Parameters
    ----------
    address : str | tuple[float,float]
        The address or (longitude, latitude) tuple to search for nearby places.
    columns : list[str] | None, optional
        Specific columns to include in the result.
    filters : FilterStructure | None, optional
        Filter criteria to apply to the results.
        Should be a list in the format(column,operator,value)
        Supported operators are: "==", "!=", "<", "<=", ">", ">=","contains"
    distance : float, optional
        Radius of the bounding box around the address. Defaults to 500 meters.
    unit : str, optional
        Unit of measurement for the distance. Defaults to "m" (meters). One of: "m","km","in","ft","yd","mi"
    release : str, optional
        Dataset release version to use. Defaults to the latest version.
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing address data within the specified bounding box of the address
    """
    return from_address(address,OVERTURE_ADDRESSES_PREFIX,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters,distance,unit)

def overture_addresses_from_place(address: str,columns: list[str]| None=None,filters: FilterStructure| None=None,release: str=OVERTURE_LATEST_RELEASE)-> GeoDataFrame:
    """
    Retrieve address data from Overture by its address or place name.
    
    Parameters
    ----------
    address : str
        The place name to search within
    columns : list[str] | None, optional
        Specific columns to include in the result.
    filters : FilterStructure | None, optional
        Filter criteria to apply to the results.
        Should be a list in the format(column,operator,value)
        Supported operators are: "==", "!=", "<", "<=", ">", ">=","contains"
    release : str, optional
        Dataset release version to use. Defaults to the latest version.
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing address data within the specified place
    """
    return from_place(address,OVERTURE_ADDRESSES_PREFIX,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters)

def overture_addresses_from_bbox(bbox: tuple[float,float,float,float],columns: list[str]| None=None,filters: FilterStructure| None=None,release: str=OVERTURE_LATEST_RELEASE)-> GeoDataFrame:
    """
    Retrieve address data from Overture within a bounding box.
    
    Parameters
    ----------
    bbox : tuple[float, float, float, float]
        The bounding box coordinates (min_x, min_y, max_x, max_y)
    columns : list[str] | None, optional
        Specific columns to include in the result.
    filters : FilterStructure | None, optional
        Filter criteria to apply to the results.
        Should be a list in the format(column,operator,value)
        Supported operators are: "==", "!=", "<", "<=", ">", ">=","contains"
    release : str, optional
        Dataset release version to use. Defaults to the latest version.
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing address data within the specified bounding box
    """
    return from_bbox(bbox,OVERTURE_ADDRESSES_PREFIX,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters)

def overture_base_from_address(address: str | tuple[float,float],base_type: str,columns: list[str]| None = None,filters: FilterStructure| None = None,distance: float = 500 ,unit: str = "m" ,release: str = OVERTURE_LATEST_RELEASE) -> GeoDataFrame:
    """
    Retrieve base data of a specific type from Overture in a bounding box around a specified address.
    
    Parameters
    ----------
    address : str | tuple[float,float]
        The address or (longitude, latitude) tuple to search for nearby places.
    base_type : str
        The type of base data to retrieve. One of: "bathymetry","infrastructure","land","land_cover",land_use","water".
    columns : list[str] | None, optional
        Specific columns to include in the result.
    filters : FilterStructure | None, optional
        Filter criteria to apply to the results.
        Should be a list in the format(column,operator,value)
        Supported operators are: "==", "!=", "<", "<=", ">", ">=","contains"
    distance : float, optional
        Radius of the bounding box around the address. Defaults to 500 meters.
    unit : str, optional
        Unit of measurement for the distance. Defaults to "m" (meters). One of: "m","km","in","ft","yd","mi"
    release : str, optional
        Dataset release version to use. Defaults to the latest version.
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing base data of the specified type within the specified bounding box of the address.
    """
    _check_base_type(base_type)
    complete_prefix = OVERTURE_BASE_PREFIX.format(type=base_type)
    return from_address(address,complete_prefix,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters,distance,unit)
    

def overture_base_from_place(address: str,base_type: str,columns: list[str]| None=None,filters: FilterStructure | None =None,release: str=OVERTURE_LATEST_RELEASE)-> GeoDataFrame:
    """
    Retrieve base data of a specific type from Overture by its address or place name.
    
    Parameters
    ----------
    address : str
        The place name to search within
    base_type : str
        The type of base data to retrieve. One of: "bathymetry","infrastructure","land","land_cover",land_use","water"
    columns : list[str] | None, optional
        Specific columns to include in the result.
    filters : FilterStructure | None, optional
        Filter criteria to apply to the results.
        Should be a list in the format(column,operator,value)
        Supported operators are: "==", "!=", "<", "<=", ">", ">=","contains"
    release : str, optional
        Dataset release version to use. Defaults to the latest version.
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing base data of the specified type within the place
    """
    _check_base_type(base_type)
    complete_prefix = OVERTURE_BASE_PREFIX.format(type=base_type)
    return from_place(address,complete_prefix,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters)
    

def overture_base_from_bbox(bbox: tuple[float,float,float,float],base_type: str,columns: list[str]| None=None,filters: FilterStructure| None=None,release: str=OVERTURE_LATEST_RELEASE)-> GeoDataFrame  :
    """
    Retrieve base data of a specific type from Overture within a bounding box.
    
    Parameters
    ----------
    bbox : tuple[float, float, float, float]
        The bounding box coordinates (min_x, min_y, max_x, max_y)
    base_type : str
        The type of base data to retrieve. One of: "bathymetry","infrastructure","land","land_cover",land_use","water"
    columns : list[str] | None, optional
        Specific columns to include in the result.
    filters : FilterStructure | None, optional
        Filter criteria to apply to the results.
        Should be a list in the format(column,operator,value)
        Supported operators are: "==", "!=", "<", "<=", ">", ">=","contains"
    release : str, optional
        Dataset release version to use. Defaults to the latest version.
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing base data of the specified type within the bounding box
    """
    _check_base_type(base_type)
    complete_prefix = OVERTURE_BASE_PREFIX.format(type=base_type)
    return from_bbox(bbox,complete_prefix,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters)

def get_categories() -> DataFrame:
    df = read_csv(OVERTURE_CATEGORIES_URL, converters={" Overture Taxonomy": lambda x : x.strip().strip("[]").split(",")}, sep=';')
    
    # Create a dictionary to store unique IDs for each category
    category_ids = {}
    
    all_records = []
    
    for _, row in df.iterrows():
        category_hierarchy = row[" Overture Taxonomy"]
        
        # Only create a record for the last/deepest level in the hierarchy
        deepest_level = len(category_hierarchy)
        current_category = category_hierarchy[deepest_level - 1]
        
        for category in category_hierarchy:
            if category not in category_ids:
                u_id = str(uuid4()).replace('-', '')[:24]
                while u_id in category_ids.values():
                    u_id = str(uuid4()).replace('-', '')[:24]
                category_ids[category] = u_id
        
        # Build the full path string
        path_parts = []
        for category in category_hierarchy:
            formatted_part = ' '.join(word.capitalize() for word in category.split('_'))
            path_parts.append(formatted_part)
        
        full_path = ' > '.join(path_parts)
        
        record = {
            'category_id': category_ids[current_category],
            'category_level': deepest_level,
            'category_name': current_category,
            'category_label': full_path,
            'level1_category_id': None,
            'level1_category_name': None,
            'level2_category_id': None,
            'level2_category_name': None,
            'level3_category_id': None,
            'level3_category_name': None,
            'level4_category_id': None,
            'level4_category_name': None,
            'level5_category_id': None,
            'level5_category_name': None,
            'level6_category_id': None,
            'level6_category_name': None
        }
        
        # Fill in the level fields based on the hierarchy
        for j in range(min(deepest_level, 6)):
            level_name = category_hierarchy[j]
            level_id = category_ids[level_name]
            
            record[f'level{j+1}_category_id'] = level_id
            record[f'level{j+1}_category_name'] = level_name
        
        all_records.append(record)
    
    result_df = DataFrame(all_records)
    
    column_order = [
        'category_id', 'category_level', 'category_name', 'category_label',
        'level1_category_id', 'level1_category_name',
        'level2_category_id', 'level2_category_name',
        'level3_category_id', 'level3_category_name',
        'level4_category_id', 'level4_category_name',
        'level5_category_id', 'level5_category_name',
        'level6_category_id', 'level6_category_name'
    ]
    
    
    available_columns = [col for col in column_order if col in result_df.columns]
    
    result_df = result_df[available_columns]
    
    return result_df

def get_schema(dataset_name : str,
                connector : bool = False,
                building_part : bool = False,base_type : str = None,release: str = OVERTURE_LATEST_RELEASE):
    """
    Get Arrow schema for the given dataset.

    Parameters
    ----------
        dataset_name : str
            Name of the dataset to get the schema of, must be one of \"buildings\",\"transportation\",\"base\",\"places\",\"addresses\"
        connector : bool, optional
            Whether to retrieve connector schema. Defaults to False.
        building_part : bool, optional 
            Whether to retrieve building_part schema. Defaults to False.
        base_type : str, optional 
            Which base type schema to retrive.
        release : str, optional 
            Release version to use, defaults to the latest version.
    Returns
    -------
    str
        String representation of PyArrow schema of dataset
    """
    datasets = ["buildings","transportation","base","places","addresses"]
    with resources.files("pyplaces").joinpath("releases/overture/base_types.txt").open( "r",encoding="utf-8-sig") as f:
        base_types = [line.replace("type=", "").strip(" \n/") for line in f]
    if dataset_name not in datasets:
        raise KeyError(f"No dataset: {dataset_name} found")
    elif connector and dataset_name != "transportation":
        raise KeyError("Dataset must be \"transportation\" to get connector schema")
    elif building_part and dataset_name != "buildings":
        raise KeyError("Dataset must be \"buildings\" to get building_part schema")
    elif base_type and dataset_name != "base":
        raise KeyError(f"Dataset must be \"base\" to get {base_type} schema")
    elif base_type and base_type not in base_types:
        raise KeyError(f"No base type:{dataset_name} found")
    
    path = OVERTURE_MAIN_PATH.format(release=release).replace("s3://", "") 
    if dataset_name == "places":
        path = path + OVERTURE_PLACES_PREFIX
    elif dataset_name == "addresses":
        path = path + OVERTURE_ADDRESSES_PREFIX
    elif dataset_name == "buildings":
        if building_part:
            path = path + OVERTURE_BUILDINGS_PART_PREFIX
        else:
            path = path + OVERTURE_BUILDINGS_PREFIX
    elif dataset_name == "base":
        path = path + OVERTURE_BASE_PREFIX.format(type=base_type)
    elif dataset_name == "transportation":
        if connector:
            path = path + OVERTURE_TRANSPORTATION_CONNECTOR_PREFIX
        else:
            path = path + OVERTURE_TRANSPORTATION_SEGMENT_PREFIX
    # print(path)
    schema = schema_from_dataset(path,OVERTURE_REGION)
    return schema.to_string()

def find_categories(search: str, num_results: int = 5, exact_match: bool=False,verbose: bool=False,as_df: bool= False) -> Union[list[str],DataFrame]:
    """
    Finds Overture Places categories based on a user search.
    
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
        Matched Overture Category names as a list of strings or DataFrame.
    """
    finder = CategoryFinder()
    categories = get_categories()
    finder.load_data(categories)
    finder.process_data()
    matches = finder.suggest_categories(search,num_results,exact_match,verbose,as_df,hide_ids=True,list_return="name",show_name_and_id=False)
    return matches

#TODO no automation
def _check_base_type(base_type):
    """
    Validate if the provided base type is valid.
    
    Parameters
    ----------
    base_type : str
        The base type to validate
        
    Raises
    ------
    ValueError
        If the base type is not valid
    """   
    with resources.files("pyplaces").joinpath("releases/overture/base_types.txt").open( "r",encoding="utf-8-sig") as f:
        base_types = [line.replace("type=", "").strip(" \n/") for line in f]
    if base_type not in base_types:
        raise ValueError(f"Invalid base type:{base_type}")
    
def _check_release(release):
    """
    Validate if the provided Overture release is valid.
    
    Parameters
    ----------
    release : str
        The release version to validate
        
    Raises
    ------
    ValueError
        If the release version is not valid
    """
    with resources.files("pyplaces").joinpath("releases/overture/releases.txt").open( "r",encoding="utf-8-sig") as f:
        folders = [line.strip(" \n/") for line in f]
    if release not in folders:
        raise ValueError(f"Invalid release:{release}")
    


__all__ = ["overture_addresses_from_address","overture_addresses_from_bbox","overture_addresses_from_place","overture_base_from_address", 
            "overture_base_from_bbox","overture_base_from_place","overture_buildings_from_address","overture_buildings_from_bbox",
            "overture_buildings_from_place","overture_places_from_address","overture_places_from_bbox","overture_places_from_place",
            "overture_transportation_from_address","overture_transportation_from_bbox","overture_transportation_from_place","get_schema","find_categories","get_categories"]

wrap_functions_with_release(__name__, _check_release,__all__)
