# pyplaces

**pyplaces is still in development, so if you run into any bugs please report them [here](https://github.com/tbanken/pyplaces/issues)**

**pyplaces** is a Python package meant to streamline the usage of large places datasets, notably Overture Maps and Foursquare Open Places. All of the datasets and past releases can be downloaded to a specific address, place or bounding box. Conventionally, users download the data themselves or use a tool such as DuckDB to download what they need, whereas pyplaces is more integrated, faster, and more precise. 

## Quickstart

### Installation

This package is available on PyPI as well as conda-forge:

    pip install pyplaces

    conda install pyplaces

I recommend that you use Anaconda for your Python environments. (I use miniforge and mamba)

### Basic Usage

To access any places, all you need is a place, address, or bounding box:

    from pyplaces.foursquare_open_places import foursquare_places_from_address

    hemenway_st_places = foursquare_places_from_address("204 Hemenway Street, Boston, MA")

Additionally, you can:
- [Select and filter on columns using the \(column,operator,value\) syntax](https://pyplaces.readthedocs.io/en/latest/usage.html#filters)
- [Get data from past releases](https://pyplaces.readthedocs.io/en/latest/usage.html#versions)
- [Inspect dataset schemas](https://pyplaces.readthedocs.io/en/latest/usage.html#schemas)
- [Find relevant category names for retrieving places data](https://pyplaces.readthedocs.io/en/latest/usage.html#categories)

Most, if not all, of the functions follow the same type of workflow.

## Documentation

The documentation is available [here](https://pyplaces.readthedocs.io/en/latest/) in case you run into any issues. 

## License

pyplaces is open source and licensed under the MIT license. 

## pyplaces Usage

This package uses a geocoder that uses OpenStreetMaps's geocoding service Nominatim. Please abide by their usage policy [here](https://operations.osmfoundation.org/policies/nominatim).

## Future Support

- more readable schema output

- reduce time in the pyarrow parquet reading pipeline

- speed up model transactions- current implementation is inefficient

- support more geoparquet datasets

- basic network analysis for transportation data
