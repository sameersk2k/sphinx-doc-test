Data
=====

WTK-LED 1224
------------

- **WIND Toolkit Large Ensemble Dataset (WTK-LED)**. WTK-LED is the recent successor to the WIND Toolkit. This model was developed and run at NREL using the updated version of Weather Research and Forecasting (WRF) model. The model setup was defined by a steering committee spanning NREL, Argonne National Laboratory, Pacific Northwest National Laboratory and the University of Colorado, Boulder and included the Mellor-Yamada-Nakanishi-Niino planetary boundary layer scheme and the unified Noah land-surface model. The forcing mechanism were sourced from ERA5.
- A 20-year WTK-LED dataset (named "WTK-LED Climate") for the years 2001 to 2020 was released in 2024 as the wind climatology component of the WTK-LED.
- WTK-LED Climate covers **North America** at a **4 km** horizontal spatial resolution and 1 hour temporal resolution.
- **WTK-LED 1224** is a derived product from the WTK-LED Climate dataset. It contains hourly average values for each hour of the day across all 12 months, resulting in 288 rows per location (12 months × 24 hours) for each year. This format was created by aggregating the full hourly WTK-LED Climate data to provide a compact, climatological representation of typical diurnal and seasonal wind patterns.
- WTK-LED 1224 retains the same **~2.51 million** grid locations as the full WTK-LED Climate dataset, preserving the original 4 km spatial resolution across North America.

**WTK-LED 1224 S3 Bucket File Structure**

- **wtk-led/1224/year=\<year>/varset={all,highwind,lowwind,midwind}/index=\<index>/\<index>\_\<year>\_\<varset>.csv.gz**
- Data is available for the years 2001 to 2020.
- Currently, varset has data with "all".
- **index** is a unique hexadecimal string with 6 characters that maps to each grid location (latitude, longitude). 
- Each file (27ab5c_2001_all.csv.gz) is approximately **28 KB** in size. Here index value 27ab5c maps to grid location/coordinate (20.954578, -61.076538). This information will help user in estimating cost of the requests.

**Note** : In future this package aims to provide additional data sources like **ERA5** to the users.