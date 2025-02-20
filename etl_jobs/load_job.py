import sys
import pandas as pd
import numpy as np
import boto3
from awsglue.utils import getResolvedOptions
#import redshift_connector

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

redshift_client = boto3.client('redshift-data')
redshift_client.excecute_statement(
    ClusterIdentifier='', #cluster
    Database='', #nome_db
    DbUser='', #utenza db
    sql="""""" #comando sql da eseguire
)

#VERIFICARE SU DOC UFFICIALE LA GESTIONE DEL METODO