import pyodbc
import pandas as pd
import configparser
from azure.storage.blob import ContainerClient
import os
import json

# Get the full path to the config.ini file
config = configparser.ConfigParser()
config.read("/Users/gomes/Desktop/Projects/Data Engineer/4-Project/config/config.ini")

# Azure Blob Storage account credentials
connection_string = config.get("blob", "STORAGE_CONNECTION_STRING")
container_name = "datalake"

# Azure Synapse SQL Pool credentials
server = config.get("synapses", "server")
database = config.get("synapses", "database")
username = config.get("synapses", "username")
password = config.get("synapses", "password")
table_name = "events"

# Connect to Blob Storage
container_client = ContainerClient.from_connection_string(connection_string, container_name)

# Download the Parquet file from the 'process' folder
parquet_file_name = 'output.parquet'
parquet_blob_name = f"process/{parquet_file_name}"
with open(parquet_file_name, 'wb') as f:
    blob_data = container_client.download_blob(parquet_blob_name).readall()
    f.write(blob_data)

# Connection string
conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Function to create the table
def create_table(connection):
    cursor = connection.cursor()
    
    create_table_query = f'''
    CREATE TABLE {table_name} (
        event_type NVARCHAR(50),
        video_id NVARCHAR(50),
        user_id NVARCHAR(50),
        created_at DATETIME,
        description NVARCHAR(4000),
        music NVARCHAR(4000),
        views FLOAT,
        likes FLOAT,
        comments_count FLOAT,
        shares FLOAT,
        EventProcessedUtcTime DATETIME,
        PartitionId INT,
        EventEnqueuedUtcTime DATETIME,
        sentiment NVARCHAR(50),
        confidence_scores NVARCHAR(4000),
        comment_id NVARCHAR(50),
        comment_text NVARCHAR(4000)
    )
    '''

    cursor.execute(create_table_query)
    cursor.commit()

# Function to insert data
def insert_data(connection, table_name, dataframe):
    cursor = connection.cursor()

    for index, row in dataframe.iterrows():
        row['confidence_scores'] = json.dumps(row['confidence_scores'])
        row['created_at'] = pd.to_datetime(row['created_at'])  # Convert the 'created_at' field
        row['EventProcessedUtcTime'] = pd.to_datetime(row['EventProcessedUtcTime'])  # Convert the 'EventProcessedUtcTime' field
        row['EventEnqueuedUtcTime'] = pd.to_datetime(row['EventEnqueuedUtcTime'])  # Convert the 'EventEnqueuedUtcTime' field
        
        # Convert the 'views', 'likes', 'comments_count', and 'shares' fields
        row['views'] = pd.to_numeric(row['views'], errors='coerce') if not pd.isna(row['views']) else None
        row['likes'] = pd.to_numeric(row['likes'], errors='coerce') if not pd.isna(row['likes']) else None
        row['comments_count'] = pd.to_numeric(row['comments_count'], errors='coerce') if not pd.isna(row['comments_count']) else None
        row['shares'] = pd.to_numeric(row['shares'], errors='coerce') if not pd.isna(row['shares']) else None

        insert_query = f'''
        INSERT INTO {table_name} (
            event_type, video_id, user_id, created_at, description, music, views, likes, comments_count, shares,
            EventProcessedUtcTime, PartitionId, EventEnqueuedUtcTime, sentiment, confidence_scores, comment_id, comment_text
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        try:
            cursor.execute(insert_query, tuple(row))
        except pyodbc.ProgrammingError as e:
            print(f"Error: {e}")
            print(f"Problematic row: {row}")
    
    cursor.commit()



# Read your parquet file into a DataFrame (replace 'output.parquet' with your actual parquet file)
data = pd.read_parquet(parquet_file_name)

# Delete the temporary local file
os.remove(parquet_file_name)

# Connect to the Azure Synapse SQL Pool
connection = pyodbc.connect(conn_str, autocommit=True)

# Create the table (run this only once)
create_table(connection)

# Insert the data into the table
insert_data(connection, table_name, data)


# Close the connection
connection.close()
