import pandas as pd
from .client_base import client_base

class WindwattsWTKClient(client_base):
    """
    This class is called "WindwattsWTKClient" because the source data is called "Wind ToolKit" and it is used by "Windwatts" application.
    """
    def __init__(self, config_path: str = None):
        # Load configuration
        super().__init__(config_path, data='wtk')
        self.df : pd.DataFrame = None
        self.current_lat : float = None 
        self.current_long : float = None
        self.global_avg : float = None
        self.yearly_avg  : float= None
        self.monthly_avg : float = None
        self.hourly_avg : float = None
        self.current_height : int = None

    def pre_check(self, 
        lat: float = None,
        long: float = None,
        height: int = None):

        if lat is None or not isinstance(lat, (float, int)):
            raise ValueError("Latitude (lat) is required and must be a float or int.")
    
        if long is None or not isinstance(long, (float, int)):
            raise ValueError("Longitude (long) is required and must be a float or int.")
        
        if not height or not isinstance(height,int):
            raise TypeError("Parameter height of int type is required.")
        
        if height not in self.column_mapping.keys():
            raise ValueError("Invalid height. Use get_column_names() method to see valid heights in the data.")
    
    def reset_averages_if_height_changes(self, height: int):
        """ Resets all stored averages when the height changes """
        if self.current_height != height:
            self.global_avg = None
            self.yearly_avg = None
            self.monthly_avg = None
            self.hourly_avg = None
            self.current_height = height  # Update stored height

    def fetch_data(self,
        lat: float = None,
        long: float = None) -> bool:
        """
        Fetch windspeed column timeseries data for all years for a given location.

        :param lat: Latitude of the location. This parameter is required.
        :type lat: float
        :param long: Longitude of the location. This parameter is required.
        :type long: float
        :return: A pandas DataFrame containing all the columns(e.g. windspeed_30m, windspeed_100m, winddirection_30m, winddirection_100m, year, varset, index).
        :rtype: bool
        """
        
        if lat is None or not isinstance(lat, (float, int)):
            raise ValueError("Latitude (lat) is required and must be a float or int.")
    
        if long is None or not isinstance(long, (float, int)):
            raise ValueError("Longitude (long) is required and must be a float or int.")
        
        # Prevent re-fetching if the coordinates are the same and data is already loaded when fetch_data is directly called.
        if self.df is not None and self.current_lat == lat and self.current_long == long:
            print("Data is already up-to-date for these coordinates.")
            return False
        
        self._reset_index_(lat,long)

        query = f"SELECT * FROM {self.athena_table_name} WHERE 1=1"
        query += f" AND index in ('{self.find_nearest_location(lat, long)}')"
        
        df = self.query_athena(query, reduce_poll=True)

        self.df = df
        self.current_lat = lat
        self.current_long = long

        return True
    
    def get_data(self, lat: float, long: float) -> pd.DataFrame:
        """
        Wrapper method around fetch_data that returns the stored DataFrame (self.df)
        if it exists and matches the provided coordinates; otherwise, it fetches the data.

        Parameters
        ----------
        lat : float
            Latitude of the location.
        long : float
            Longitude of the location.

        Returns
        -------
        pd.DataFrame
            The DataFrame containing the timeseries wind data.
        """
        # Check if self.df exists and the stored coordinates match the provided ones.
        if self.df is not None and self.current_lat == lat and self.current_long == long:
            return self.df
        # Otherwise, fetch data and return the updated DataFrame.
        self.fetch_data(lat, long)
        return self.df

    def fetch_global_avg_at_height(self,
        lat: float = None,
        long: float = None,
        height: int = None) -> dict:
        """
        Calculate global windspeed average for specified height, filtered by a single location.
        Returns stored averages if fetch_data() does not fetch new data.

        Parameters
        ----------
        :param lat: Latitude (required).
        :param long: Longitude (required).
        :param int: Hub Height (required).

        Returns
        -------
        dict
        A JSON object with the following keys:
        - `global_avg` (float): The overall average wind speed for the specified location and height, rounded to 2 decimals.
        """
        
        self.pre_check(lat, long, height)
        self.reset_averages_if_height_changes(height)

        try:
            is_data_fetched = self.fetch_data(lat, long)
        except Exception as e:
            self.df = None  # Reset df to avoid using stale data
            raise RuntimeError("Failed to fetch timeseries data.") from e

        if self.df is None or self.df.empty:
            raise RuntimeError("No data available after fetching.")
        
        # Return stored value if no new data was fetched
        if not is_data_fetched and self.global_avg is not None:
            return {"global_avg": self.global_avg}
        
        try:
            self.global_avg = float(round(self.df[f'windspeed_{height}m'].mean(),2))
        except Exception as e:
            raise RuntimeError(f"Failed to calculate global average for windspeed_{height}m.") from e
        
        return {
            "global_avg": self.global_avg
        }
    
    def fetch_yearly_avg_at_height(self,
        lat: float = None,
        long: float = None,
        height: int = None) -> dict:
        """
        Calculate yearly windspeed averages for specified height, filtered by a single location.
        Returns stored averages if fetch_data() does not fetch new data.

        Parameters
        ----------
        :param lat: Latitude (required).
        :param long: Longitude (required).
        :param height: Hub Height (required).

        Returns
        -------
        dict
        A JSON object with the following keys:
        - `yearly_avg` (list of dicts): A list of dictionaries with `year` (int) and average windspeed for that year (float).
            Example:
            [
                {"year": 2020, "windspeed_{height}m": 5.23},
                {"year": 2021, "windspeed_{height}m": 5.34}
            ]
        """
        
        self.pre_check(lat, long, height)
        self.reset_averages_if_height_changes(height)

        try:
            is_data_fetched = self.fetch_data(lat, long)
        except Exception as e:
            self.df = None  # Reset df to avoid using stale data
            raise RuntimeError("Failed to fetch timeseries data.") from e

        if self.df is None or self.df.empty:
            raise RuntimeError("No data available after fetching.")
        
        # Return stored value if no new data was fetched
        if not is_data_fetched and self.yearly_avg is not None:
            return {"yearly_avg": self.yearly_avg}
        
        try:
            yearly_avg_df = self.df.groupby('year')[f'windspeed_{height}m'].mean().reset_index().round(2).sort_values(by='year', ascending=True)
            self.yearly_avg = yearly_avg_df.to_dict(orient='records')
        except Exception as e:
            raise RuntimeError(f"Failed to calculate yearly average for windspeed_{height}m.") from e
        
        return {
                "yearly_avg": self.yearly_avg      
        }
    
    def fetch_monthly_avg_at_height(self,
        lat: float = None,
        long: float = None,
        height: int = None) -> dict:
        """
        Calculate monthly windspeed averages for specified height, filtered by a single location..
        Returns stored averages if fetch_data() does not fetch new data.

        Parameters
        ----------
        :param lat: Latitude (required).
        :param long: Longitude (required).
        :param height: Hub Height (required).

        Returns
        -------
        dict
        A JSON object with the following keys:
        - monthly_avg` (list of dicts): A list of dictionaries with `month` (int) and average windspeed for that month (float).
            Example:
            [
                {"month": 1, "windspeed_{height}m": 5.12},
                {"month": 2, "windspeed_{height}m": 5.45}
            ]
        """
        
        self.pre_check(lat, long, height)
        self.reset_averages_if_height_changes(height)

        try:
            is_data_fetched = self.fetch_data(lat, long)
        except Exception as e:
            self.df = None  # Reset df to avoid using stale data
            raise RuntimeError("Failed to fetch timeseries data.") from e

        if self.df is None or self.df.empty:
            raise RuntimeError("No data available after fetching.")
        
        # Return stored value if no new data was fetched
        if not is_data_fetched and self.monthly_avg is not None:
            return {"monthly_avg": self.monthly_avg}
    
        self.df['month']=self.df['mohr']//100

        try:
            monthly_avg_df = self.df.groupby('month')[f'windspeed_{height}m'].mean().reset_index().round(2).sort_values(by='month', ascending=True)
            self.monthly_avg = monthly_avg_df.to_dict(orient='records')
        except Exception as e:
            raise RuntimeError(f"Failed to calculate monthly average for windspeed_{height}m.") from e
        
        return {
                "monthly_avg": self.monthly_avg        
        }
    
    def fetch_hourly_avg_at_height(self,
        lat: float = None,
        long: float = None,
        height: int = None) -> dict:
        """
        Calculate hourly windspeed averages for specified height, filtered by a single location..
        Returns stored averages if fetch_data() does not fetch new data.

        Parameters
        ----------
        :param lat: Latitude (required).
        :param long: Longitude (required).
        :param height: Hub Height (required).

        Returns
        -------
        dict
        A JSON object with the following keys:
        - `hourly_avg` (list of dicts): A list of dictionaries with `hour` (int) and average windspeed for that hour (float).
            Example:
            [
                {"hour": 1, "windspeed_{height}m": 4.98},
                {"hour": 2, "windspeed_{height}m": 5.10}
            ]
        """
        
        self.pre_check(lat, long, height)
        self.reset_averages_if_height_changes(height)

        try:
            is_data_fetched = self.fetch_data(lat, long)
        except Exception as e:
            self.df = None  # Reset df to avoid using stale data
            raise RuntimeError("Failed to fetch timeseries data.") from e

        if self.df is None or self.df.empty:
            raise RuntimeError("No data available after fetching.")
        
        # Return stored value if no new data was fetched
        if not is_data_fetched and self.hourly_avg is not None:
            return {"hourly_avg": self.hourly_avg}
        
        self.df['hour'] = self.df['mohr']%100

        try:
            hourly_avg_df = self.df.groupby('hour')[f'windspeed_{height}m'].mean().reset_index().round(2).sort_values(by='hour', ascending=True)
            self.hourly_avg = hourly_avg_df.to_dict(orient='records')  
        except Exception as e:
            raise RuntimeError(f"Failed to calculate hourly average for windspeed_{height}m.") from e
        
        return {
                "hourly_avg": self.hourly_avg   
        }