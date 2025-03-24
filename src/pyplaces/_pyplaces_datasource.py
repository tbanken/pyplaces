from abc import ABC, abstractmethod
from geopandas import GeoDataFrame
from ._utils import get_gdf_from_bbox,geocode_point_to_bbox,geocode_place_to_bbox,FilterStructure

class pyplaces_datasource(ABC):
        
    def from_address(self,address: str,prefix: str,main_path: str, region: str,release: str,columns: list[str]| None = None,filters: FilterStructure| None = None,distance: float = 500 ,unit: str = "m")-> GeoDataFrame:
        self.check_release(release)
        bbox = geocode_point_to_bbox(address,distance,unit)
        gdf = get_gdf_from_bbox(release,bbox,columns,filters,prefix,main_path,region)
        return gdf
    
    def from_place(self,address: str,prefix: str,main_path: str, region: str,release: str,columns: list[str]| None=None,filters: FilterStructure| None=None)-> GeoDataFrame:
        self.check_release(release)
        geometry,bbox = geocode_place_to_bbox(address)
        gdf = get_gdf_from_bbox(release,bbox,columns,filters,prefix,main_path,region)
        filtered_gdf = gdf[gdf.within(geometry)]
        return filtered_gdf
    
    def from_bbox(self,bbox: tuple[float,float,float,float],prefix: str,main_path: str, region: str,release: str,columns: list[str]| None=None,filters: FilterStructure| None=None)-> GeoDataFrame:
        self.check_release(release)
        gdf = get_gdf_from_bbox(release,bbox,columns,filters,prefix,main_path,region)
        return gdf

    @abstractmethod
    def check_release(self,release):
        """Check user-input release validity (datasource-specific)."""
