import sys
import pandas as pd
import boto3
from datetime import timedelta
from awsglue.utils import getResolvedOptions
from io import BytesIO

args = getResolvedOptions(sys.argv, 
                          ['JobName',
                           'BucketName',
                           'SourcePath',
                           'TargetPath', 
                           'PricesDataset', 
                           'TrendDataset', 
                           'Crypto'])

job_name = args['JobName']
bucket_name = args['BucketName']
source = args['SourcePath']
target = args['TargetPath']
crypto = args['Crypto']

prices = args['PricesDataset']
trend = args['TrendDataset']

def prices_processing(data):
    """
    Processes a Parquet file containing price data and returns a DataFrame 
    with some columns removed and a new 'Price' column that represents 
    the moving average of prices over a 10-observation interval.

    Args:
        data (str): The path to the Parquet file to be processed.

    Returns:
        pd.DataFrame: A DataFrame containing the processed data, 
                      with unnecessary columns removed and 
                      the 'Price' column modified.
    """
    df = pd.read_parquet(data, engine="pyarrow")
    df.drop(columns=['Open', 'High', 'Low', 'Vol.[K]', 'Change %'], inplace=True)
    df['Price'] = df['Price'].rolling(window=10, min_periods=1).mean()
    return df

def trend_processing(data):
    df = pd.read_parquet(data, engine='pyarrow')
    df['Week'] = pd.to_datetime(df['Week'])

    #inizializzazione di una lista vuota per le nuove righe del df
    rows = []

    #si itera per ciascuna riga del dataframe originale
    for _, row in df.iterrows():
        week_end_date = row['Week']
        #calcolo del primo giorno della settimana
        week_start__date = week_end_date - timedelta(days=6)

        #aggiunta dei valori per i giorni della settimana
        for i in range(7):
            day = week_start__date + timedelta(days=i)
            rows.append({"Date": day, "Google_trend_index": row['Google_trend_index']})

    new_df = pd.DataFrame(rows)
    
    return new_df

def join_dataframes(df1, df2, join_col, join_type):
    """
    Performs a join between two DataFrames.

    Parameters:
    df1 (pd.DataFrame): The first DataFrame.
    df2 (pd.DataFrame): The second DataFrame.
    on (str): Name of the column to join on.
    how (str): Type of join: 'inner', 'left', 'right', 'outer'. Default is 'inner'.

    Returns:
    pd.DataFrame: The DataFrame resulting from the join operation.
    """
    df = pd.merge(df1, df2, how=join_type, on=join_col)
    return df

file_list = [prices, trend]

s3 = boto3.client('s3')

#per ciascuno dei file si esegue il preprocessing
for file in file_list:
    response = s3.get_object(Bucket=bucket_name, Key=source+file)
    data = response['Body'].read()
    data_io = BytesIO(data)

    if file == prices:
        price_df = prices_processing(data_io)
    else:
        trend_df = trend_processing(data_io)

#si effettua la join tra prezzi e trend
df = join_dataframes(price_df, trend_df, join_col='Date', join_type='inner')

#si salva il risultato in un file parquet nella directory target
local_parquet = local_parquet_file = r"/tmp/"+ crypto + ".parquet"
df.to_parquet(local_parquet_file, engine="pyarrow", index=False)
s3.upload_file(local_parquet_file, bucket_name, target+ crypto + ".parquet")