import sys
import redshift_connector
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, 
                          ['JobName',
                           'BucketName',
                           'SourcePath',
                           'Crypto',
                           'IamRole',
                           'RedshiftCluster',
                           'RedshiftDatabase',
                           'DbSchema',
                           'DbUser',
                           'DbPwd',
                           'DbPort'])

job_name = args['JobName']
bucket_name = args['BucketName']
source = args['SourcePath']
iam_role = args['IamRole']
crypto = args['Crypto']
cluster = args['RedshiftCluster']
db = args['RedshiftDatabase']
db_schema = args['DbSchema']
db_user = args['DbUser']
db_pwd = args['DbPwd']
db_port = int(args['DbPort'])

#definizione della query per la creazione della tabella di destinazione
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {db_schema}.{crypto} (
    Date DATE,
    Price FLOAT,
    Google_trend_index BIGINT
    );
    """

#definizione della query per l'inserimento dei dati in tabella
insert_values_sql = f"""
COPY {db_schema}.{crypto}
FROM 's3://{bucket_name}/{source}{crypto}.parquet'
IAM_ROLE '{iam_role}'
FORMAT AS PARQUET;
"""

#inizializzazione della connessione al datawarehouse
with redshift_connector.connect(host=cluster, database=db, user=db_user, 
                                password=db_pwd, port=db_port) as conn:
    with conn.cursor() as cursor:
        #creazione della tabella
        cursor.execute(create_table_sql)

        #inserimento dei dati
        cursor.execute(insert_values_sql)

        #inserimento del commit
        conn.commit()