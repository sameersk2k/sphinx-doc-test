import boto3
from botocore.exceptions import BotoCoreError, ClientError
import pandas as pd
import numpy as np
import time
import gzip
import pickle
import os
import json
from importlib.resources import files
from io import BytesIO
from scipy.spatial import cKDTree

class client_base:
    
    def __init__(self, config_path=None, data :str = None):

        # Parameter validation for config_path
        if config_path is None:
            raise ValueError("A valid config_path must be provided.")
        if not isinstance(config_path, str):
            raise ValueError("config_path must be a string.")
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        self.data = data
        
        # Parameter validation for data
        allowed_data = {"wtk", "era5"}
        if self.data is None:
            # Default to 'wtk' if no data type is provided.
            self.data = "wtk"
        elif not isinstance(self.data, str):
            raise ValueError("data parameter must be a string.")
        elif self.data.lower() not in allowed_data:
            raise ValueError(f"data parameter must be one of {allowed_data}")
        
        # Load configuration from a file if provided
        self.config = self._load_config(config_path)
        self.s3 = boto3.client('s3', region_name=self.config.get('region_name'))
        self.bucket_name = self.config.get('bucket_name')
        self.athena = boto3.client('athena', region_name=self.config.get('region_name'))
        self.database = self.config.get('database')
        self.output_location = self.config.get('output_location')
        self.output_bucket = self.config.get('output_bucket')
        
        # Path for location index file for wtk 1224 data
        self.wtk_preprocessed_file_path = str(files('windwatts_data').joinpath('data/location_data.pkl.gz'))

        # Load athena table with respect to the data source
        if self.data == 'era5':
            self.default_athena_table_name = self.config.get('era5_athena_table_name')
        else:
            self.default_athena_table_name = self.config.get('wtk_athena_table_name')

        self.alt_athena_table_name = self.config.get('alt_athena_table_name')
        self.athena_table_name = self.default_athena_table_name
        self.athena_workgroup=self.config.get('athena_workgroup')
        self.location_gdf = None
        self.kdtree = None
        self.column_mapping=None
        self.column_names = self._initialize_column_names()
        self._load_preprocessed_data()
        self._initialize_column_mapping()
        self.build_kdtree()
        
    
    def _load_config(self, config_path):
        """Load configuration from a JSON file."""
        with open(config_path, 'r') as f:
            return json.load(f)
    
    def _initialize_column_names(self):
        """
        Run the DESCRIBE query once during initialization to get column names.
        :return: A list of column names.
        """
        query = f"DESCRIBE {self.athena_table_name}"
        raw_results = self.query_athena(query, convert_to_dataframe=False)

        # Extract column names from the raw results
        data = raw_results['data']
        column_names = [row[0].split('\t')[0].strip() for row in data if row]
        # Find the stopping point dynamically
        try:
            last_valid_index = column_names.index('index') + 1
            column_names = column_names[:last_valid_index]  # Keep only valid columns
        except ValueError:
            pass
        return column_names
    
    def _initialize_column_mapping(self):
        """
        Preprocess the all_columns list into a dictionary grouped by height.
        """
        column_mapping = {}
        for col in self.column_names:
                try:
                    if col.endswith('m') and self.data == 'wtk':
                        height = int(col.split('_')[-1][:-1])  # Extract numeric height for wtk data
                    else:
                        height = int(col[2:]) # Extract numeric height for era5 data
                    if height not in column_mapping:
                        column_mapping[height] = []
                    column_mapping[height].append(col)
                except ValueError:
                    continue  # Skip invalid heights
        self.column_mapping = column_mapping
        
    
    def _load_preprocessed_data(self):
        """
        Load the location index data with respect to the data source from package.
        """
        if self.location_gdf is None:
            if self.data == 'wtk':
                with gzip.open(self.wtk_preprocessed_file_path, 'rb') as f:
                    self.location_gdf = pickle.load(f)
            else:
                raise Exception("Location index file for {self.data} is not yet available in this package.")

    def build_kdtree(self):
        """Precompute KDTree for fast nearest neighbor search."""
        if self.kdtree is None:
            coords = np.vstack((self.location_gdf.geometry.x, self.location_gdf.geometry.y)).T
            self.kdtree = cKDTree(coords)
        
    def find_nearest_location(self, user_lat, user_long):
        """
        Find the nearest location using a KDTree for extremely fast searches.
        :param user_lat: User's latitude
        :param user_long: User's longitude
        :return: The indexes of the n nearest locations.
        """
        if self.location_gdf is None:
            self._load_preprocessed_data()
        
        if self.kdtree is None:
            self.build_kdtree()
        
        _, nearest_idx = self.kdtree.query([user_long, user_lat])
        
        return self.location_gdf.iloc[nearest_idx]['index']
    
    def find_n_nearest_locations(self, user_lat, user_long, n):
        """
        Find the n nearest locations using a KDTree for extremely fast searches.
        :param user_lat: User's latitude
        :param user_long: User's longitude
        :param n: Number of nearest locations/indexes to find.
        :return: The indexes of the n nearest locations.
        """
        if self.location_gdf is None:
            self._load_preprocessed_data()

        if self.kdtree is None:
            self.build_kdtree()
        
        # Query KDTree for the N nearest neighbors
        _, nearest_idxs = self.kdtree.query([user_long, user_lat], k=n)  # k=N for multiple nearest

        return self.location_gdf.iloc[nearest_idxs]['index'].tolist()
    

    def query_athena(self, query_string, convert_to_dataframe=True, return_result_location_only=False, reduce_poll=False) -> pd.DataFrame:
        """
        Executes an Athena query and fetches results as a Pandas DataFrame or raw data.

        :param query_string: The SQL query to execute.
        :param convert_to_dataframe: If True, converts results into a Pandas DataFrame.
        :param return_result_location_only: If True, returns the S3 result location only.
        :param reduce_poll: If True, reduces query status poll time to 0.1 Sec else default is 0.5 Sec.
        :return: Pandas DataFrame (if convert_to_dataframe=True) or raw results (if False).
        :raises RuntimeError: If the Athena query fails or encounters an AWS error.
        """

        try:
            # Start query execution
            response = self.athena.start_query_execution(
                QueryString=query_string,
                QueryExecutionContext={'Database': self.database},
                ResultConfiguration={'OutputLocation': self.output_location},
                ResultReuseConfiguration={
                    'ResultReuseByAgeConfiguration': {'Enabled': True, 'MaxAgeInMinutes': 10080}
                },
                WorkGroup = self.athena_workgroup
            )
            query_execution_id = response['QueryExecutionId']

            # Poll query status with exponential backoff
            status = 'RUNNING'
            if not reduce_poll:
                wait_time = 0.5
            else:
                wait_time = 0.1
            while status in ['RUNNING', 'QUEUED']:
                response = self.athena.get_query_execution(QueryExecutionId=query_execution_id)
                status = response['QueryExecution']['Status']['State']
                if status in ['RUNNING', 'QUEUED']:
                    time.sleep(wait_time)
                    wait_time = min(wait_time * 2, 5)  # Cap the backoff at 5 seconds

            # Handle query completion or failure
            if status == 'SUCCEEDED':
                result_location = response['QueryExecution']['ResultConfiguration']['OutputLocation']

                # Return S3 location only
                if return_result_location_only:
                    return f"File is too large to return at runtime. Download the result from {result_location}."

                # Handle raw results
                if not convert_to_dataframe:
                    paginator = self.athena.get_paginator('get_query_results')
                    all_rows = []
                    columns = None

                    for page in paginator.paginate(QueryExecutionId=query_execution_id):
                        if columns is None:
                            columns = [col['Label'] for col in page['ResultSet']['ResultSetMetadata']['ColumnInfo']]
                        all_rows.extend(page['ResultSet']['Rows'])

                    data = [
                        [field.get('VarCharValue', None) for field in row['Data']]
                        for row in all_rows
                    ]
                    return {'columns': columns, 'data': data}

                # Fetch and process results into a DataFrame
                bucket, key = result_location.replace("s3://", "").split("/", 1)
                csv_obj = self.s3.get_object(Bucket=bucket, Key=key)
                df_chunks = pd.read_csv(BytesIO(csv_obj['Body'].read()), dtype={'index': str}, chunksize=100000)
                df = pd.concat(df_chunks, ignore_index=True)
                #df = self._convert_dataframe_types(df)
                return df

            elif status == 'FAILED':
                error_message = response['QueryExecution']['Status']['StateChangeReason']
                raise RuntimeError(f"Athena query failed with error: {error_message}")

            elif status == 'CANCELLED':
                raise RuntimeError("Athena query was cancelled.")

            else:
                raise RuntimeError(f"Athena query ended with unexpected status: {status}")

        except (BotoCoreError, ClientError) as e:
            raise RuntimeError(f"AWS error occurred: {e}")

        except Exception as e:
            raise RuntimeError(f"Unexpected error: {e}")


    def get_column_names(self):
        if self.column_names is None:
            self.column_names=self._initialize_column_names()
        
        return self.column_names
    
    def find_relevant_columns(self,heights):
        if self.column_mapping is None:
            self._initialize_column_mapping()
        available_heights = sorted(self.column_mapping.keys())  # Get available heights
        relevant_columns = []

        for height in heights:
            if height in self.column_mapping:
                # Exact match for height
                relevant_columns.extend(self.column_mapping[height])
            else:
                # Use upper and lower logic
                lower = max((h for h in available_heights if h <= height), default=None)
                upper = min((h for h in available_heights if h >= height), default=None)
                if lower:
                    relevant_columns.extend(self.column_mapping[lower])
                if upper and upper != lower:
                    relevant_columns.extend(self.column_mapping[upper])

        # Deduplicate columns while preserving order
        columns = list(sorted(dict.fromkeys(relevant_columns)))
        return columns
    
    def map_index_to_coordinates(self,df: pd.DataFrame = None) -> pd.DataFrame:
        """
        Map the index column of retrieved timeseries or map data from other functions to latitude and longitude.

        :param df: Dataframe with index column. (Required)
        :type df: pandas.DataFrame
        :return: A pandas DataFrame containing existing columns along with latitude and longitude columns mapped to index column.
        :rtype: pandas.DataFrame
        """
        if df is None:
            raise ValueError("Please provide a pandas dataframe with index column to map it to latitude and longitude.")
        if self.location_gdf is None:
            self._load_preprocessed_data()
        
        spatial_data = self.location_gdf.set_index('index').loc[df['index']]
        # Add latitude and longitude to your DataFrame
        df['latitude'] = spatial_data['latitude'].values
        df['longitude'] = spatial_data['longitude'].values

        return df
    
    def _reset_index_(self,lat,long):
        '''
        Ensures column names are initialized based on query type(location based and non-location based.)
        '''
        if self.column_names is None:
            try:
                self.column_names = self._initialize_column_names()
            except Exception as e:
                raise RuntimeError("Failed to initialize column names. Check your configuration.") from e

        if lat is not None and long is not None:
            self.athena_table_name = self.default_athena_table_name
            if 'index' not in self.column_names:
                self.column_names.append('index')
        else:
            self.athena_table_name = self.alt_athena_table_name
            if 'index' in self.column_names:
                self.column_names.remove('index')
    
    def get_location_gdf(self) -> pd.DataFrame:
        '''
        returns location_gdf of the respective class to investigate grid locations if needed.

        '''
        if self.location_gdf is None:
            self._load_preprocessed_data()
        
        return self.location_gdf