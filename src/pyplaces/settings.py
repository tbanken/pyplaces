from __future__ import annotations

import logging as lg
from typing import TYPE_CHECKING
from typing import Any

if TYPE_CHECKING:
    from pathlib import Path

nominatim_url: str = "https://nominatim.openstreetmap.org/"


#configure 

use_cache: bool = True

cache_folder: str | Path = "./cache"
cache_only_mode: bool = False
data_folder: str | Path = "./data"
default_crs: str = "epsg:4326"

http_accept_language: str = "en"
http_referer: str = "Ted Banken" # OSMnx Python package (https://github.com/gboeing/osmnx)
http_user_agent: str = "Ted Banken"

requests_kwargs: dict[str, Any] = {}
requests_timeout: float = 180

log_console: bool = False
log_file: bool = False
log_filename: str = "pyplaces"
log_level: int = lg.INFO
log_name: str = "pyplaces"
logs_folder: str | Path = "./logs"

max_query_area_size: float = 50 * 1000 * 50 * 1000
