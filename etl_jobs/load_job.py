import sys
import pandas as pd
import numpy as np
import boto3
from awsglue.utils import getResolvedOptions
from io import BytesIO

args = getResolvedOptions(sys.argv, 
                          ['JobName',
                           'BucketName',
                           'SourcePath',
                           'RedshiftCluster',
                           'RedshiftDatabase', 
                           'DbSchema', 
                           'DbUser', 
                           'DbPwd',
                           'DbPort'])

job_name = args['JobName']
bucket_name = args['BucketName']
source = args['SourcePath']
target = args['TargetPath']
crypto = args['Crypto']
catalog_db = args['DataCatalogDb']

prices = args['PricesDataset']
trend = args['TrendDataset']

#definizione della query per la creazione della tabella di destinazione
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS crypto.{crypto} (
    Date DATE,
    Price DECIMAL(10,2),
    Google_trend_index INT
    )"""

#definizione della query per l'inserimento dei dati in tabella
insert_values_sql = f"""
INSERT INTO crypto.{crypto} (Date, Price, Google_trend_index) VALUES
()
"""

redshift_client = boto3.client('redshift-data')
redshift_client.excecute_statement(
    ClusterIdentifier='', #cluster
    Database='', #nome_db
    DbUser='', #utenza db
    sql="""""" #comando sql da eseguire
)

#VERIFICARE SU DOC UFFICIALE LA GESTIONE DEL METODO