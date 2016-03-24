# Data Observatory

This module contains a series of PLPGSQL functions designed to make joining
your data with data in the Data Observatory seamless.

## Search functions

- [OBS_Search](40_observatory/OBS_SEARCH.md) - Search the Data Observatory for a dimension
- [OBS_List_Geom_columns](40_observatory/OBS_LIST_GEOM_COLUMNS.md) List the geometries that are available on the Data Observatory

## Augmentation functions

- [OBS_Augment_Census](40_observatory/OBS_Augment_census.md) - For a given input geometry get the request census dimension aggregated in a sensible way.

- [OBS_Augment_table_with_census](40_observatory/OBS_Augment_table_with_census.md) - For a given input table append a column with the requested census dimension, aggregated in a sensible way.
