from geopandas import GeoDataFrame
from ._overture_maps import overture_maps, OVERTURE_LATEST_RELEASE
from ._foursquare_open_places import foursquare_open_places, FSQ_LATEST_RELEASE
from ._utils import FilterStructure

class PyPlaces:
    def __init__(self):
        self.__overture = overture_maps()
        self.__foursquare = foursquare_open_places()
    
    def overture_places_from_address(self,address: str,columns: list[str]| None = None,filters: FilterStructure| None = None,distance: float = 500 ,unit: str = "m",release:str=OVERTURE_LATEST_RELEASE) -> GeoDataFrame:
        return self.__overture.overture_places_from_address(address,columns,filters,distance,unit,release)

    def overture_places_from_place(self,address: str,columns: list[str]| None=None,filters: FilterStructure| None=None,release:str=OVERTURE_LATEST_RELEASE)-> GeoDataFrame:
        return self.__overture.overture_places_from_place(address,columns,filters,release)

    def overture_places_from_bbox(self,bbox: tuple[float,float,float,float],columns: list[str]| None=None,filters: FilterStructure| None=None,release:str=OVERTURE_LATEST_RELEASE)-> GeoDataFrame:
        return self.__overture.overture_places_from_bbox(bbox,columns,filters,release)
    
    def overture_buildings_from_address(self,address: str,columns: list[str]| None = None,filters: FilterStructure| None = None,distance: float = 500 ,unit: str = "m" ,release: str =OVERTURE_LATEST_RELEASE,building_part: bool=False) -> GeoDataFrame:
        return self.__overture.overture_buildings_from_address(address,columns,filters,distance,unit,release,building_part)
    
    def overture_buildings_from_place(self,address: str,columns: list[str]| None=None,filters: FilterStructure| None=None,release:str=OVERTURE_LATEST_RELEASE,building_part: bool=False)-> GeoDataFrame:
        return self.__overture.overture_buildings_from_place(address,columns,filters,release,building_part)

    def overture_buildings_from_bbox(self,bbox: tuple[float,float,float,float],columns: list[str]| None=None,filters: FilterStructure| None=None,release:str=OVERTURE_LATEST_RELEASE,building_part: bool=False)-> GeoDataFrame:
        return self.__overture.overture_buildings_from_bbox(bbox,columns,filters,release,building_part)
    
    def overture_transportation_from_address(self,address: str,columns: list[str]| None = None,filters: FilterStructure| None = None,distance: float = 500 ,unit: str = "m" ,release: str =OVERTURE_LATEST_RELEASE,connector:bool=False) -> GeoDataFrame:
        return self.__overture.overture_transportation_from_address(address,columns,filters,distance,unit,release,connector)

    def overture_transportation_from_place(self,address: str,columns: list[str]| None=None,filters: FilterStructure| None=None,release:str=OVERTURE_LATEST_RELEASE,connector:bool=False)-> GeoDataFrame:
        return self.__overture.overture_transportation_from_place(address,columns,filters,release,connector)

    def overture_transportation_from_bbox(self,bbox: tuple[float,float,float,float],columns: list[str]| None=None,filters: FilterStructure | None=None,release:str=OVERTURE_LATEST_RELEASE,connector:bool=False)-> GeoDataFrame:
        return self.__overture.overture_transportation_from_bbox(bbox,columns,filters,release,connector)
        
    def overture_addresses_from_address(self,address: str,columns: list[str]| None = None,filters: FilterStructure| None = None,distance: float = 500 ,unit: str = "m" ,release: str =OVERTURE_LATEST_RELEASE) -> GeoDataFrame:
        return self.__overture.overture_addresses_from_address(address,columns,filters,distance,unit,release)

    def overture_addresses_from_place(self,address: str,columns: list[str]| None=None,filters: FilterStructure| None=None,release:str=OVERTURE_LATEST_RELEASE)-> GeoDataFrame:
        return self.__overture.overture_addresses_from_place(address,columns,filters,release)

    def overture_addresses_from_bbox(self,bbox: tuple[float,float,float,float],columns: list[str]| None=None,filters: FilterStructure| None=None,release:str=OVERTURE_LATEST_RELEASE)-> GeoDataFrame:
        return self.__overture.overture_addresses_from_bbox(bbox,columns,filters,release)

    def overture_base_from_address(self,address: str,base_type: str,columns: list[str]| None = None,filters: FilterStructure| None = None,distance: float = 500 ,unit: str = "m" ,release:str=OVERTURE_LATEST_RELEASE) -> GeoDataFrame:
        return self.__overture.overture_base_from_address(address,base_type,columns,filters,distance,unit,release)

    def overture_base_from_place(self,address: str,base_type: str,columns: list[str]| None=None,filters: FilterStructure=None,release:str=OVERTURE_LATEST_RELEASE)-> GeoDataFrame:
        return self.__overture.overture_base_from_place(address,base_type,columns,filters,release)

    def overture_base_from_bbox(self,bbox: tuple[float,float,float,float],base_type: str,columns: list[str]| None=None,filters: FilterStructure| None=None,release:str=OVERTURE_LATEST_RELEASE)-> GeoDataFrame:
        return self.__overture.overture_base_from_bbox(bbox,base_type,columns,filters,release)
    
    def foursquare_places_from_address(self,address: str,columns: list[str]| None = None,filters: FilterStructure | None = None,distance: float = 500 ,unit: str = "m" ,release: str =FSQ_LATEST_RELEASE) -> GeoDataFrame:
        return self.__foursquare.foursquare_places_from_address(address,columns,filters,distance,unit,release)

    def foursquare_places_from_place(self,address: str,columns: list[str]| None=None,filters: FilterStructure=None,release: str=FSQ_LATEST_RELEASE)-> GeoDataFrame:
        return self.__foursquare.foursquare_places_from_place(address,columns,filters,release)

    def foursquare_places_from_bbox(self,bbox: tuple[float,float,float,float],columns: list[str]| None=None,filters: FilterStructure| None=None,release: str=FSQ_LATEST_RELEASE)-> GeoDataFrame:
        return self.__foursquare.foursquare_places_from_bbox(bbox,columns,filters,release)
