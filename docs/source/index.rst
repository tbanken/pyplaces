.. pyplaces documentation master file, created by
   sphinx-quickstart on Mon Apr 14 22:18:38 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

pyplaces |release| documentation
################################
.. note::
   pyplaces is still in development, so if you run into any bugs please report them `here <https://github.com/tbanken/pyplaces/issues/>`_.

**Pyplaces** is a Python package that allows you to easily read places data from a known address, place, or just a bounding box. Currently, the two sources are
Overture Maps and Foursquare Open Places. Overture Maps provides support to get data from multiple versions of the dataset for all of its themes. 
Foursquare Open Places is primarily point of interest data which is also supports fetching multiple versions of the dataset.

Quickstart
**********

Installation
============

This package is available on PyPI as well as conda-forge: ::

   pip install pyplaces

I recommend that you use Anaconda for your Python environments. (I use miniforge and mamba)::

   conda install pyplaces

Basic Usage
===========

To access any places, all you need is a place, address, or bounding box: ::

   from pyplaces.foursquare_open_places import foursquare_places_from_address

   hemenway_st_places = foursquare_places_from_address("204 Hemenway Street, Boston, MA")

Additionally, you can:

* Select columns
* Filter on columns using the \(column,operator,value\) syntax (see support for this syntax here)
* Get data from past releases(see support for this here)

Most, if not all, of the functions follow the same type of workflow.

User Reference
**************

The user reference is available here in case you run into any issues. 

License
*******

pyplaces is open source and licensed under the MIT license. 

pyplaces Usage
**************

This package uses a geocoder that uses OpenStreetMaps's geocoding service Nominatim. Please abide by their usage policy `here <https://operations.osmfoundation.org/policies/nominatim/>`_.

Future Support
**************

* matching for foursquare categories table(to easily retrive types of POIs)
* get schemas for each theme/dataset
* support more geoparquet datasets
* basic network analysis for transportation data

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   usage

