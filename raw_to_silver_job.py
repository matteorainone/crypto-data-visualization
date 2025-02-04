import sys
import pandas as pd
import numpy as np
import boto3
from awsglue.utils import getResolvedOptions
from io import StringIO

args = getResolvedOptions(sys.argv, 
                          ['JobName',
                           'BucketName',
                           'SourcePath',
                           'TargetPath', 
                           'PricesDataset', 
                           'TrendDataset', 
                           'Crypto',
                           'DataCatalogDb'])

job_name = args['JobName']
bucket_name = args['BucketName']
source = args['SourcePath']
target = args['TargetPath']
crypto = args['Crypto']
catalog_db = args['DataCatalogDb']

prices = args['PricesDataset']
trend = args['TrendDataset']

def price_substitution(df):
    """
    Replaces invalid price values (-1) in the dataset with the rolling mean of the last 10 observations.

    Args:
        df (pd.DataFrame): DataFrame containing a 'Price' column.

    Returns:
        pd.DataFrame: Updated DataFrame with invalid price values replaced.
    """
    df['rolling_mean'] = df['Price'].rolling(window=10, min_periods=1).mean()
    df.loc[df['Price'] == -1, 'Price'] = df.loc[df['Price'] == -1, 'rolling_mean']
    df.drop(columns=['rolling_mean'], inplace=True)
    
    return df

def price_dataset_preprocessing(price_dataset_file:str):
    """
    Preprocesses a price dataset from a CSV file and returns a Pandas DataFrame with transformed data.

    Args:
        price_dataset_file (str): Path to the CSV file containing the price dataset.

    Returns:
        pd.DataFrame: A Pandas DataFrame with the following transformations applied:
            - Converts the 'Date' column to datetime format.
            - Cleans and converts the 'Vol.' column to numeric (replacing 'K' with the corresponding thousand value).
            - Fills NaN values in the 'Vol.[K]' column using a rolling mean of the last 10 observations.
            - Removes the '%' symbol from the 'Change %' column and converts it to float.
    """
    #import data in Pandas Dataframe
    df = pd.read_csv(price_dataset_file, parse_dates=['Date'], thousands=',')

    ###### Vol. column preprocessing ######################################
    
    #string conversion and NaN replace
    df['Vol.'] = df['Vol.'].astype(str).replace({'nan': np.nan, '': np.nan})
    df.rename(columns={"Vol.":"Vol.[K]"}, inplace=True)
    #float conversion
    df['Vol.[K]'] = df['Vol.[K]'].str.replace('K', '', regex=False).astype(float)
    #nan values are replaced with the rolling mean of the last 10 observations
    df['Vol.[K]'] = df['Vol.[K]'].fillna(df['Vol.[K]'].rolling(window=10, min_periods=1).mean())

    ######## Change % col. preprocessing ##########################
    df['Change %'] = df['Change %'].map(lambda x:x.replace('%',''))
    df['Change %'] = df['Change %'].map(lambda x: float(x))

    return df

def label_normalizer(trend_dataset_file:str):
    """
    Normalizes column names in a Google Trends dataset.

    Args:
        trend_dataset_file (str): Path to the CSV file containing the trend dataset.

    Returns:
        pd.DataFrame: A Pandas DataFrame with standardized column names:
            - The first column is renamed to 'Week'.
            - The second column is renamed to 'Google_trend_index'.
    """
    df = pd.read_csv(trend_dataset_file, parse_dates=['Settimana'])
    columns = df.columns
    df.rename(columns={columns[0]:"Week", columns[1]:"Google_trend_index"}, inplace=True)
    return df

def create_or_update_table(database_name, table_name, s3_location, columns):
    glue_client = boto3.client('glue')

    # Definisci lo Storage Descriptor per la tabella
    storage_descriptor = {
        'Columns': columns,  # ad es. [{'Name': 'col1', 'Type': 'string'}, ...]
        'Location': s3_location,
        'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
        'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
        'SerdeInfo': {
            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
            'Parameters': {'serialization.format': '1'}
        }
    }

    table_input = {
        'Name': table_name,
        'StorageDescriptor': storage_descriptor,
        'TableType': 'EXTERNAL_TABLE',
    }

    try:
        print(f"Creazione della tabella '{table_name}' nel database '{database_name}'...")
        response = glue_client.create_table(
            DatabaseName=database_name,
            TableInput=table_input
        )
        print("Tabella creata con successo.")
    except glue_client.exceptions.AlreadyExistsException:
        print("La tabella esiste già. Provo ad aggiornarla...")
        response = glue_client.update_table(
            DatabaseName=database_name,
            TableInput=table_input
        )
        print("Tabella aggiornata con successo.")
    except Exception as e:
        print("Errore durante la creazione/aggiornamento della tabella:", e) 

def output_save(data_type, data_file, crypto, s3_client, bucket, file_destination, data_catalog:bool):
    """
    Processes and saves cryptocurrency price or trend data as a Parquet file.

    Args:
        data_type (str): Type of data to process ('prices' or other).
        data_file (str): String representation of the CSV file content.
        crypto (str): Cryptocurrency name or symbol.
        data_catalog (bool): Unused parameter (reserved for future functionality).

    Returns:
        None: The processed data is saved as a Parquet file in the /tmp directory.
    """
    if data_type == 'prices':
        df = price_dataset_preprocessing(StringIO(data_file))
        df = price_substitution(df)
    else:
        df = label_normalizer(StringIO(data_file))
        
    local_parquet_file = r"/tmp/"+ crypto + "_" + data_type + ".parquet"

    df.to_parquet(local_parquet_file, engine="pyarrow", index=False)

    s3_client.upload_file(local_parquet_file, bucket, file_destination+ crypto + "_" + data_type + ".parquet")

    if data_catalog:
        if data_type == 'prices':
            columns = [{"Name":"Date", "Type":"date"},
                       {"Name":"Price", "Type": "float"},
                       {"Name": "Open", "Type":"float"},
                       {"Name": "High", "Type":"float"},
                       {"Name": "Low", "Type":"float"},
                       {"Name": "Vol.[K]", "Type":"float"},
                       {"Name": "Change %", "Type":"float"}]
        else:
            columns = [{"Name": "Week", "Type": "date"},
                       {"Name": "Google_trend_index", "Type": "int"}]
            
        create_or_update_table(catalog_db, crypto + data_type + "clear_all", file_destination+ crypto + "_" + data_type + ".parquet", columns)

   
file_list = [prices, trend]

s3 = boto3.client('s3')

for file in file_list:
    response = s3.get_object(Bucket=bucket_name, Key=source+file)
    data = response['Body'].read().decode('utf-8')
    if file == prices:
        output_save('prices', data, crypto, s3, bucket_name, target, data_catalog=True)
    else:
        output_save('trend', data, crypto, s3, bucket_name, target, data_catalog=True)