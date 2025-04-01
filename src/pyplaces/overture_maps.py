from geopandas import GeoDataFrame
from importlib import resources
from ._utils import FilterStructure, from_address, from_bbox, from_place

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

def overture_places_from_address(address: str,columns: list[str]| None = None,filters: FilterStructure| None = None,distance: float = 500 ,unit: str = "m" ,release: str = OVERTURE_LATEST_RELEASE) -> GeoDataFrame:
    return from_address(address,OVERTURE_PLACES_PREFIX,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters,distance,unit)

def overture_places_from_place(address: str,columns: list[str]| None=None,filters: FilterStructure| None=None,release: str=OVERTURE_LATEST_RELEASE)-> GeoDataFrame:
    return from_place(address,OVERTURE_PLACES_PREFIX,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters)

def overture_places_from_bbox(bbox: tuple[float,float,float,float],columns: list[str]| None=None,filters: FilterStructure| None=None,release: str=OVERTURE_LATEST_RELEASE)-> GeoDataFrame:
    return from_bbox(bbox,OVERTURE_PLACES_PREFIX,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters)

def overture_buildings_from_address(address: str,columns: list[str]| None = None,filters: FilterStructure| None = None,distance: float = 500 ,unit: str = "m" ,release: str = OVERTURE_LATEST_RELEASE,building_part: bool=False) -> GeoDataFrame:
    if building_part:
        prefix = OVERTURE_BUILDINGS_PART_PREFIX
    else:
        prefix = OVERTURE_BUILDINGS_PREFIX 
    return from_address(address,prefix,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters,distance,unit)

def overture_buildings_from_place(address: str,columns: list[str]| None=None,filters: FilterStructure| None=None,release: str=OVERTURE_LATEST_RELEASE,building_part: bool=False)-> GeoDataFrame:
    if building_part:
        prefix = OVERTURE_BUILDINGS_PART_PREFIX
    else:
        prefix = OVERTURE_BUILDINGS_PREFIX
    return from_place(address,prefix,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters)

def overture_buildings_from_bbox(bbox: tuple[float,float,float,float],columns: list[str]| None=None,filters: FilterStructure| None=None,release: str=OVERTURE_LATEST_RELEASE,building_part: bool=False)-> GeoDataFrame:
    if building_part:
        prefix = OVERTURE_BUILDINGS_PART_PREFIX
    else:
        prefix = OVERTURE_BUILDINGS_PREFIX
    return from_bbox(bbox,prefix,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters)

def overture_transportation_from_address(address: str,columns: list[str]| None = None,filters: FilterStructure| None = None,distance: float = 500 ,unit: str = "m" ,release: str = OVERTURE_LATEST_RELEASE,connector:bool=False) -> GeoDataFrame:
    if connector:
        prefix = OVERTURE_TRANSPORTATION_CONNECTOR_PREFIX
    else:
        prefix = OVERTURE_TRANSPORTATION_SEGMENT_PREFIX
    return from_address(address,prefix,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters,distance,unit)

def overture_transportation_from_place(address: str,columns: list[str]| None=None,filters: FilterStructure| None=None,release: str=OVERTURE_LATEST_RELEASE,connector:bool=False)-> GeoDataFrame:
    if connector:
        prefix = OVERTURE_TRANSPORTATION_CONNECTOR_PREFIX
    else:
        prefix = OVERTURE_TRANSPORTATION_SEGMENT_PREFIX
    return from_place(address,prefix,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters)

def overture_transportation_from_bbox(bbox: tuple[float,float,float,float],columns: list[str]| None=None,filters: FilterStructure | None=None,release: str=OVERTURE_LATEST_RELEASE,connector:bool=False)-> GeoDataFrame:
    if connector:
        prefix = OVERTURE_TRANSPORTATION_CONNECTOR_PREFIX
    else:
        prefix = OVERTURE_TRANSPORTATION_SEGMENT_PREFIX
    return from_bbox(bbox,prefix,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters)
    
def overture_addresses_from_address(address: str,columns: list[str]| None = None,filters: FilterStructure| None = None,distance: float = 500 ,unit: str = "m" ,release: str = OVERTURE_LATEST_RELEASE) -> GeoDataFrame:
    return from_address(address,OVERTURE_ADDRESSES_PREFIX,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters,distance,unit)

def overture_addresses_from_place(address: str,columns: list[str]| None=None,filters: FilterStructure| None=None,release: str=OVERTURE_LATEST_RELEASE)-> GeoDataFrame:
    return from_place(address,OVERTURE_ADDRESSES_PREFIX,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters)

def overture_addresses_from_bbox(bbox: tuple[float,float,float,float],columns: list[str]| None=None,filters: FilterStructure| None=None,release: str=OVERTURE_LATEST_RELEASE)-> GeoDataFrame:
    return from_bbox(bbox,OVERTURE_ADDRESSES_PREFIX,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters)

def overture_base_from_address(address: str,base_type: str,columns: list[str]| None = None,filters: FilterStructure| None = None,distance: float = 500 ,unit: str = "m" ,release: str = OVERTURE_LATEST_RELEASE) -> GeoDataFrame:
    check_base_type(base_type)
    complete_prefix = OVERTURE_BASE_PREFIX.format(base_type=base_type)
    return from_address(address,complete_prefix,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters,distance,unit)
    

def overture_base_from_place(address: str,base_type: str,columns: list[str]| None=None,filters: FilterStructure=None,release: str=OVERTURE_LATEST_RELEASE)-> GeoDataFrame:
    check_base_type(base_type)
    complete_prefix = OVERTURE_BASE_PREFIX.format(base_type=base_type)
    return from_place(address,complete_prefix,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters)
    

def overture_base_from_bbox(bbox: tuple[float,float,float,float],base_type: str,columns: list[str]| None=None,filters: FilterStructure| None=None,release: str=OVERTURE_LATEST_RELEASE)-> GeoDataFrame  :
    check_base_type(base_type)
    complete_prefix = OVERTURE_BASE_PREFIX.format(base_type=base_type)
    return from_bbox(bbox,complete_prefix,OVERTURE_MAIN_PATH,OVERTURE_REGION,release,columns,filters)

def check_release(release):
    with resources.files("pyplaces").joinpath("releases/overture/releases.txt").open( "r",encoding="utf-8-sig") as f:
        folders = [line.strip(" \n/") for line in f]
    if release not in folders:
        raise ValueError(f"Invalid release:{release}")

#TODO no automation
def check_base_type(base_type):   
    with open("releases/overture/base_types.txt", "r",encoding="utf-8-sig") as f:
        folders = [line.replace("type=", "").strip(" \n/") for line in f]
    if base_type not in folders:
        raise ValueError(f"Invalid base type:{base_type}")
