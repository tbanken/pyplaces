# pyplaces

**pyplaces** is a Python package meant to streamline the usage of large places datasets, notably Overture Maps and Foursquare Open Places. All of the datasets and past releases can be downloaded to a specific address, place or bounding box. Conventionally, users download the data themselves or use a tool such as DuckDB to download what they need, whereas pyplaces is more integrated, faster, and more precise. 

## Quickstart

### Installation

This package is available on PyPI.

[//]: # (This package is available on PyPI as well as conda-forge:)

    pip install pyplaces

[//]: # (conda install pyplaces)

[//]: # (I recommend you use Anaconda for your Python environments.)

### Basic Usage

To access any places, all you need is a place, address, or bounding box:

    from pyplaces.foursquare_open_places import foursquare_places_from_address

    hemenway_st_places = foursquare_places_from_address("204 Hemenway Street, Boston, MA")

Additionally, you can:
- Select columns
- Filter on columns using the \(column,operator,value\) syntax (see support for this syntax here)
- Get data from past releases(see support for this here)

## User Reference

The user reference is available here in case you run into any issues. 
## License

pyplaces is open source and licensed under the MIT license. 

## pyplaces Usage

This package uses a geocoder that uses OpenStreetMaps's geocoding service Nominatim. Please abide by their usage policy here.

## Future Support

- matching for foursquare categories table(to easily retrive types of POIs)

- get schemas for each theme/dataset

- support more geoparquet datasets

- basic network analysis for transportation data
