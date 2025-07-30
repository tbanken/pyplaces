
.. _user-ref:
User Reference
################################

Below you can find the reference for the package modules, as well as some additional information for using them.

Dataset Information
*******************

.. _schemas:

Dataset Schemas and Additional Information
===============

Schemas can be retrieved from either dataset by using :code:`get_schema`

Additional schema and dataset information can be found below.

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

Filtering and Accessing Dictionary Columns
============================

The filter structure is based off of DuckDB comparision functionality which can be found `here. <https://duckdb.org/docs/stable/sql/expressions/overview>`_
**This is only the raw SQL, no need to insert "WHERE." Anything that can be in the "WHERE" statement can go into this filter**

Certain columns within the datasets will be a dictionary, most notably, the Overture Places column "categories."
This column is set up as a dictionary: :code:`{"primary":"<category>","secondary":["<category2>","<category3>",...]}`

In order to filter by the dictionary values, use a "." in your filter. More information on that `here. <https://duckdb.org/docs/stable/sql/data_types/struct.html>`_

.. _categories:

Finding Categories
=======

Finding categories for a place can be challenging because of the accessibility of the category names and codes.(which can be found below)

The :code:`find_categories` function for each dataset can help. You can enter in a search query(e.g., finding hardware stores by searching "hardware store").
It uses exact and semantic matching to find relevant categories to the search. The quality of the results depend on the detail of your search. 

* `Foursquare Open Places Categories <https://docs.foursquare.com/data-products/docs/categories#places-open-source--propremium-flat-file>`_
* `Overture Places Categories <https://github.com/OvertureMaps/schema/blob/main/docs/schema/concepts/by-theme/places/overture_categories.csv>`_


.. _versions:

Versions
============

Each dataset is continuously updated with new information. One of the goals of pyplaces is to be able to reference historic editions of all the datasets supported. 

Overture Maps Releases
======================
+---------------------+----------------------+
| Release Date        |  Unsupported Themes  |
+=====================+======================+
| 2025-07-23.0        |           X          |
+---------------------+----------------------+
| 2025-06-25.0        |           X          |
+---------------------+----------------------+
| 2025-05-21.0        |           X          |
+---------------------+----------------------+
| 2025-04-23.0        |           X          |
+---------------------+----------------------+
| 2025-03-19.1        |           X          |
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
| 2025-07-08  |
+-------------+
| 2025-06-10  |
+-------------+
| 2025-05-09  |
+-------------+
| 2025-04-08  |
+-------------+
| 2025-03-06  |
+-------------+
| 2025-02-06  |
+-------------+
| 2025-01-10  |
+-------------+
| 2024-12-03  |
+-------------+
| 2024-11-19  |
+-------------+
