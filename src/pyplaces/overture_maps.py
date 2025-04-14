"""Functions to fetch  geoparquet data from Overture Maps on AWS"""
from geopandas import GeoDataFrame
from importlib import resources
from ._utils import FilterStructure, wrap_functions_with_release
from ._io_utils import from_address, from_bbox, from_place

#TODO latest release reads from text file
OVERTURE_MAIN_PATH = 's3://overturemaps-us-west-2/release/{release}/'
OVERTURE_BUCKET = 'overturemaps-us-west-2'
OVERTURE_REGION = 'us-west-2'
OVERTURE_LATEST_RELEASE = "2025-01-22.0"

OVERTURE_PLACES_PREFIX = "theme=places/type=place/"
OVERTURE_BUILDINGS_PREFIX = "theme=buildings/type=building/"
OVERTURE_BUILDINGS_PART_PREFIX = "theme=buildings/type=building_part/"
OVERTURE_ADDRESSES_PREFIX = "theme=addresses/type=address/"
OVERTURE_TRANSPORTATION_SEGMENT_PREFIX = "theme=transportation/type=segment/"
OVERTURE_TRANSPORTATION_CONNECTOR_PREFIX = "theme=transportation/type=connector/"
OVERTURE_BASE_PREFIX = "theme=base/type={type}"

def overture_places_from_address(address: str,columns: list[str]| None = None,filters: FilterStructure | None = None,distance: float = 500 ,unit: str = "m" ,release: str = OVERTURE_LATEST_RELEASE) -> GeoDataFrame:
    """
    Retrieve places data from Overture based on an address.
    
    Parameters
    ----------
    address : str
        The address to search from
    columns : list[str] | None, optional
        Specific columns to include in the result, by default None
    filters : FilterStructure | None, optional
        Filters to apply to the data, by default None
    distance : float, optional
        Search radius from the address, by default 500
    unit : str, optional
        Unit for the distance ('m' for meters, etc.), by default "m"
    release : str, optional
        Overture data release version, by default OVERTURE_LATEST_RELEASE
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing places data around the specified address
    """
    return from_address(address,OVERTURE_PLACES_PREFIX,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters,distance,unit)

def overture_places_from_place(address: str,columns: list[str]| None=None,filters: FilterStructure| None=None,release: str=OVERTURE_LATEST_RELEASE)-> GeoDataFrame:
    """
    Retrieve places data from Overture based on a named place.
    
    Parameters
    ----------
    address : str
        The place name to search within
    columns : list[str] | None, optional
        Specific columns to include in the result, by default None
    filters : FilterStructure | None, optional
        Filters to apply to the data, by default None
    release : str, optional
        Overture data release version, by default OVERTURE_LATEST_RELEASE
        
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
        Specific columns to include in the result, by default None
    filters : FilterStructure | None, optional
        Filters to apply to the data, by default None
    release : str, optional
        Overture data release version, by default OVERTURE_LATEST_RELEASE
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing places data within the specified bounding box
    """
    return from_bbox(bbox,OVERTURE_PLACES_PREFIX,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters)

def overture_buildings_from_address(address: str,columns: list[str]| None = None,filters: FilterStructure| None = None,distance: float = 500 ,unit: str = "m" ,release: str = OVERTURE_LATEST_RELEASE,building_part: bool=False) -> GeoDataFrame:
    """
    Retrieve buildings data from Overture based on an address.
    
    Parameters
    ----------
    address : str
        The address to search from
    columns : list[str] | None, optional
        Specific columns to include in the result, by default None
    filters : FilterStructure | None, optional
        Filters to apply to the data, by default None
    distance : float, optional
        Search radius from the address, by default 500
    unit : str, optional
        Unit for the distance ('m' for meters, etc.), by default "m"
    release : str, optional
        Overture data release version, by default OVERTURE_LATEST_RELEASE
    building_part : bool, optional
        Whether to retrieve building parts instead of whole buildings, by default False
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing buildings data around the specified address
    """
    if building_part:
        prefix = OVERTURE_BUILDINGS_PART_PREFIX
    else:
        prefix = OVERTURE_BUILDINGS_PREFIX 
    return from_address(address,prefix,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters,distance,unit)

def overture_buildings_from_place(address: str,columns: list[str]| None=None,filters: FilterStructure| None=None,release: str=OVERTURE_LATEST_RELEASE,building_part: bool=False)-> GeoDataFrame:
    """
    Retrieve buildings data from Overture based on a named place.
    
    Parameters
    ----------
    address : str
        The place name to search within
    columns : list[str] | None, optional
        Specific columns to include in the result, by default None
    filters : FilterStructure | None, optional
        Filters to apply to the data, by default None
    release : str, optional
        Overture data release version, by default OVERTURE_LATEST_RELEASE
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
        Specific columns to include in the result, by default None
    filters : FilterStructure | None, optional
        Filters to apply to the data, by default None
    release : str, optional
        Overture data release version, by default OVERTURE_LATEST_RELEASE
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

def overture_transportation_from_address(address: str,columns: list[str]| None = None,filters: FilterStructure| None = None,distance: float = 500 ,unit: str = "m" ,release: str = OVERTURE_LATEST_RELEASE,connector:bool=False) -> GeoDataFrame:
    """
    Retrieve transportation data from Overture based on an address.
    
    Parameters
    ----------
    address : str
        The address to search from
    columns : list[str] | None, optional
        Specific columns to include in the result, by default None
    filters : FilterStructure | None, optional
        Filters to apply to the data, by default None
    distance : float, optional
        Search radius from the address, by default 500
    unit : str, optional
        Unit for the distance ('m' for meters, etc.), by default "m"
    release : str, optional
        Overture data release version, by default OVERTURE_LATEST_RELEASE
    connector : bool, optional
        Whether to retrieve connectors instead of segments, by default False
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing transportation data around the specified address
    """
    if connector:
        prefix = OVERTURE_TRANSPORTATION_CONNECTOR_PREFIX
    else:
        prefix = OVERTURE_TRANSPORTATION_SEGMENT_PREFIX
    return from_address(address,prefix,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters,distance,unit)

def overture_transportation_from_place(address: str,columns: list[str]| None=None,filters: FilterStructure| None=None,release: str=OVERTURE_LATEST_RELEASE,connector:bool=False)-> GeoDataFrame:
    """
    Retrieve transportation data from Overture based on a named place.
    
    Parameters
    ----------
    address : str
        The place name to search within
    columns : list[str] | None, optional
        Specific columns to include in the result, by default None
    filters : FilterStructure | None, optional
        Filters to apply to the data, by default None
    release : str, optional
        Overture data release version, by default OVERTURE_LATEST_RELEASE
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
        Specific columns to include in the result, by default None
    filters : FilterStructure | None, optional
        Filters to apply to the data, by default None
    release : str, optional
        Overture data release version, by default OVERTURE_LATEST_RELEASE
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
    
def overture_addresses_from_address(address: str,columns: list[str]| None = None,filters: FilterStructure| None = None,distance: float = 500 ,unit: str = "m" ,release: str = OVERTURE_LATEST_RELEASE) -> GeoDataFrame:
    """
    Retrieve address data from Overture based on an address.
    
    Parameters
    ----------
    address : str
        The address to search from
    columns : list[str] | None, optional
        Specific columns to include in the result, by default None
    filters : FilterStructure | None, optional
        Filters to apply to the data, by default None
    distance : float, optional
        Search radius from the address, by default 500
    unit : str, optional
        Unit for the distance ('m' for meters, etc.), by default "m"
    release : str, optional
        Overture data release version, by default OVERTURE_LATEST_RELEASE
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing address data around the specified address
    """
    return from_address(address,OVERTURE_ADDRESSES_PREFIX,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters,distance,unit)

def overture_addresses_from_place(address: str,columns: list[str]| None=None,filters: FilterStructure| None=None,release: str=OVERTURE_LATEST_RELEASE)-> GeoDataFrame:
    """
    Retrieve address data from Overture based on a named place.
    
    Parameters
    ----------
    address : str
        The place name to search within
    columns : list[str] | None, optional
        Specific columns to include in the result, by default None
    filters : FilterStructure | None, optional
        Filters to apply to the data, by default None
    release : str, optional
        Overture data release version, by default OVERTURE_LATEST_RELEASE
        
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
        Specific columns to include in the result, by default None
    filters : FilterStructure | None, optional
        Filters to apply to the data, by default None
    release : str, optional
        Overture data release version, by default OVERTURE_LATEST_RELEASE
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing address data within the specified bounding box
    """
    return from_bbox(bbox,OVERTURE_ADDRESSES_PREFIX,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters)

def overture_base_from_address(address: str,base_type: str,columns: list[str]| None = None,filters: FilterStructure| None = None,distance: float = 500 ,unit: str = "m" ,release: str = OVERTURE_LATEST_RELEASE) -> GeoDataFrame:
    """
    Retrieve base data of a specific type from Overture based on an address.
    
    Parameters
    ----------
    address : str
        The address to search from
    base_type : str
        The type of base data to retrieve
    columns : list[str] | None, optional
        Specific columns to include in the result, by default None
    filters : FilterStructure | None, optional
        Filters to apply to the data, by default None
    distance : float, optional
        Search radius from the address, by default 500
    unit : str, optional
        Unit for the distance ('m' for meters, etc.), by default "m"
    release : str, optional
        Overture data release version, by default OVERTURE_LATEST_RELEASE
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing base data of the specified type around the address
    """
    _check_base_type(base_type)
    complete_prefix = OVERTURE_BASE_PREFIX.format(base_type=base_type)
    return from_address(address,complete_prefix,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters,distance,unit)
    

def overture_base_from_place(address: str,base_type: str,columns: list[str]| None=None,filters: FilterStructure=None,release: str=OVERTURE_LATEST_RELEASE)-> GeoDataFrame:
    """
    Retrieve base data of a specific type from Overture based on a named place.
    
    Parameters
    ----------
    address : str
        The place name to search within
    base_type : str
        The type of base data to retrieve
    columns : list[str] | None, optional
        Specific columns to include in the result, by default None
    filters : FilterStructure, optional
        Filters to apply to the data, by default None
    release : str, optional
        Overture data release version, by default OVERTURE_LATEST_RELEASE
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing base data of the specified type within the place
    """
    _check_base_type(base_type)
    complete_prefix = OVERTURE_BASE_PREFIX.format(base_type=base_type)
    return from_place(address,complete_prefix,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters)
    

def overture_base_from_bbox(bbox: tuple[float,float,float,float],base_type: str,columns: list[str]| None=None,filters: FilterStructure| None=None,release: str=OVERTURE_LATEST_RELEASE)-> GeoDataFrame  :
    """
    Retrieve base data of a specific type from Overture within a bounding box.
    
    Parameters
    ----------
    bbox : tuple[float, float, float, float]
        The bounding box coordinates (min_x, min_y, max_x, max_y)
    base_type : str
        The type of base data to retrieve
    columns : list[str] | None, optional
        Specific columns to include in the result, by default None
    filters : FilterStructure | None, optional
        Filters to apply to the data, by default None
    release : str, optional
        Overture data release version, by default OVERTURE_LATEST_RELEASE
        
    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing base data of the specified type within the bounding box
    """
    _check_base_type(base_type)
    complete_prefix = OVERTURE_BASE_PREFIX.format(base_type=base_type)
    return from_bbox(bbox,complete_prefix,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters)



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
    with open("releases/overture/base_types.txt", "r",encoding="utf-8-sig") as f:
        folders = [line.replace("type=", "").strip(" \n/") for line in f]
    if base_type not in folders:
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
    
wrap_functions_with_release(__name__, _check_release)

__all__ = ["overture_addresses_from_address","overture_addresses_from_bbox","overture_addresses_from_place","overture_base_from_address", 
            "overture_base_from_bbox","overture_base_from_place","overture_buildings_from_address","overture_buildings_from_bbox",
            "overture_buildings_from_place","overture_places_from_address","overture_places_from_bbox","overture_places_from_place",
            "overture_transportation_from_address","overture_transportation_from_bbox","overture_transportation_from_place"]
