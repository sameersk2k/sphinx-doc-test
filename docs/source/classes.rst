Classes
=======

Depending on the data on which the user wants to work with. Users can use different classes within the package.
Currently the package has 1 class:

1. :ref:`WTKLedClient1224 <WTKLedClient1224>`


.. _WTKLedClient1224:

WTKLedClient1224
----------------
This class provides functionality for working with hourly average time-series data from WTK-LED called as WTK-LED 1224. The dataset is organized such that, 
for a specific location and year, it contains 288 rows. Each row represents the hourly average values for a particular hour of the day across 
each month (12 months * 24 hours = 288 rows per file).

.. autofunction:: windwatts_data.wtk_client_1224.WTKLedClient1224.get_1224_data

.. autofunction:: windwatts_data.wtk_client_1224.WTKLedClient1224.fetch_windspeed_timeseries_1224


