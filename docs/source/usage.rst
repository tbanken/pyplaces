User Reference
################################

Below you can find the reference for the package modules, as well as some additional information for using them.

Dataset Information
*******************

Dataset Schemas and Additional Information
===============

In the future, pyplaces will support a more dynamic way to look at the schemas of each dataset. For now, the schema reference for each dataset will be listed here(as well as the main reference for each):

* `Overture Map Guide  <https://docs.overturemaps.org//>`_
* `Overture Maps Schemas <https://docs.overturemaps.org/schema/reference/>`_
* `Foursquare Open Places Guide <https://docs.foursquare.com/data-products/docs/fsq-places-open-source>`_
* `Foursquare Open Places Schemas <https://docs.foursquare.com/data-products/docs/places-os-data-schema>`_


Modules
*******

.. _pyplaces-overture_maps-module:

pyplaces.overture_maps
======================

.. automodule:: pyplaces.overture_maps
    :members:

.. _pyplaces-foursquare_open_places-module:

pyplaces.foursquare_open_places
===============================

.. automodule:: pyplaces.foursquare_open_places
    :members:


Formatting parameters
*********************

Versions
============

Each dataset is continuously updated with new information. One of the goals of pyplaces is to be able to reference historic editions of all the datasets supported. 

Overture Maps Releases
======================
+---------------------+----------------------+
| Release Date        |  Unsupported Themes  |
+=====================+======================+
| 2025-03-19.0        |           X          |
+---------------------+----------------------+
| 2025-02-19.0        |           X          |
+---------------------+----------------------+
| 2025-01-22.0        |           X          |
+---------------------+----------------------+
| 2024-12-18.0        |           X          |
+---------------------+----------------------+
| 2024-11-13.0        |           X          |
+---------------------+----------------------+
| 2024-10-23.0        |           X          |
+---------------------+----------------------+
| 2024-09-18.0        |           X          |
+---------------------+----------------------+
| 2024-08-20.0        |           X          |
+---------------------+----------------------+
| 2024-07-22.0        |           X          |
+---------------------+----------------------+
| 2024-06-13-beta.1   | addresses            |
+---------------------+----------------------+
| 2024-05-16-beta.0   | addresses            |
+---------------------+----------------------+
| 2024-04-16-beta.0   | addresses            |
+---------------------+----------------------+
| 2024-03-12-alpha.0  | addresses            |
+---------------------+----------------------+
| 2024-02-15-alpha.0  | addresses            |
+---------------------+----------------------+
| 2024-01-17-alpha.0  | addresses            |
+---------------------+----------------------+
| 2023-12-14-alpha.0  | addresses            |
+---------------------+----------------------+
| 2023-11-14-alpha.0  | addresses            |
+---------------------+----------------------+
| 2023-10-19-alpha.0  | addresses            |
+---------------------+----------------------+
| 2023-07-26-alpha.0  | base, addresses      |
+---------------------+----------------------+

Foursquare Open Places Releases
===============================
+-------------+
| Release Date|
+=============+
| 2024-11-19  |
+-------------+
| 2024-12-03  |
+-------------+
| 2025-01-10  |
+-------------+
| 2025-02-06  |
+-------------+
| 2025-03-06  |
+-------------+
| 2025-04-08  |
+-------------+


Filters
=======

Filters consist of the column that needs filtering, an equality operator,
and a value to filter on. 

Examples of valid filters:

.. code-block::

    [("column","==",2)]
    [("column1","==",2),("column2","==",0.9)] # Filters in the same list will be OR'd together
    [("column1","==",2),[(("column2","==",0.9))]] # Filters in a nested list will be AND'd together

