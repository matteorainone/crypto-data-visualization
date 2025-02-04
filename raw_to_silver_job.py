import sys
import pandas as pd
import numpy as np
import boto3
from awsglue.utils import getResolvedOptions
from io import StringIO

def price_substitution(df):
    df['rolling_mean'] = df['Price'].rolling(window=10, min_periods=1).mean()
    df.loc[df['Price'] == -1, 'Price'] = df.loc[df['Price'] == -1, 'rolling_mean']
    df.drop(columns=['rolling_mean'], inplace=True)
    
    return df

def price_dataset_preprocessing(price_dataset_file:str):
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
    df = pd.read_csv(trend_dataset_file, parse_dates=['Settimana'])
    columns = df.columns
    df.rename(columns={columns[0]:"Week", columns[1]:"Google_trend_index"}, inplace=True)
    return df

args = getResolvedOptions(sys.argv, 
                          ['JOB_NAME',
                           'bucket_name',
                           'source_path',
                           'target_path', 
                           'prices_dataset', 
                           'trend_dataset', 
                           'silver_path'])

job_name = args['JOB_NAME']
bucket_name = args['bucket_name']
source = args['source_path']
target = args['target_path']

prices = args['prices_dataset']
trend = args['trend_dataset']

file_list = [prices, trend]

s3 = boto3.client('s3')

for file in file_list:
    response = s3.get_object(Bucket=bucket_name, Key=source+file)
    data = response['Body'].read().decode('utf-8')
    if file == prices:
        price_df = price_dataset_preprocessing(StringIO(data))
        price_df = price_substitution(price_df)
    else:
        trend_df = label_normalizer(StringIO(data))
