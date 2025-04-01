"""Geographic and geometry utility functions."""

from __future__ import annotations

import numpy as np
from pyproj import Geod
from shapely import Polygon
from ._conversion_utils import convert_to_meters
from osmnx.geocoder import geocode, geocode_to_gdf
from osmnx import settings

# Set OSMNX settings
settings.http_referer = "pyplaces Python Package" 
settings.http_user_agent = "pyplaces"

def point_buffer(lon: float, lat: float, radius_m: float) -> Polygon:
    """
    Create a circular buffer around a point using the WGS84 ellipsoid.
    
    Parameters:
    -----------
    lon : float
        Longitude of the center point
    lat : float
        Latitude of the center point
    radius_m : float
        Radius of the buffer in meters
        
    Returns:
    --------
    shapely.Polygon
        Polygon representing the buffer
    """
    # Use this instead of `.buffer()` provided by geodataframe
    # Adapted from:
    # https://stackoverflow.com/questions/31492220/how-to-plot-a-tissot-with-cartopy-and-matplotlib
    geod = Geod(ellps='WGS84')
    num_vtxs = 64
    lons, lats, _ = geod.fwd(np.repeat(lon, num_vtxs),
                            np.repeat(lat, num_vtxs),
                            np.linspace(360, 0, num_vtxs),
                            np.repeat(radius_m, num_vtxs),
                            radians=False
                            )
    return Polygon(zip(lons, lats))

def geocode_point_to_bbox(address: str, distance: float, unit: str):
    """
    Convert an address or coordinates to a bounding box using a buffer.
    
    Parameters:
    -----------
    address : str or tuple
        Address string or (longitude, latitude) tuple
    distance : float
        Buffer distance
    unit : str
        Unit of distance (m, km, ft, etc.)
        
    Returns:
    --------
    tuple
        Bounding box as (minx, miny, maxx, maxy)
    """
    if isinstance(address, str):
        point = geocode(address)
    else:
        point = address
    distance = convert_to_meters(distance, unit)
    bbox = point_buffer(point[1], point[0], distance).bounds
    return bbox

def geocode_place_to_bbox(address: str):
    """
    Convert a place name to its geometry and bounding box.
    
    Parameters:
    -----------
    address : str
        Place name or address
        
    Returns:
    --------
    tuple
        (geometry, bbox) where bbox is (minx, miny, maxx, maxy)
    """
    gdf = geocode_to_gdf(query=address, which_result=1, by_osmid=False)
    row = gdf.iloc[0]
    geometry = row["geometry"]
    bbox = (row["bbox_west"], row["bbox_south"], row["bbox_east"], row["bbox_north"])
    return geometry, bbox