import os
import json
import boto3
import logging
import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

def update_lock_file(bucket, lock_key, timestamp):
    """
    Aggiorna (o crea) il file di lock scrivendo il timestamp (in formato ISO).
    """
    ts_str = timestamp.isoformat()
    try:
        s3_client.put_object(Bucket=bucket, Key=lock_key, Body=ts_str)
        logger.info("Lock file %s aggiornato con timestamp: %s", lock_key, ts_str)
    except Exception as e:
        logger.error("Errore nell'aggiornamento del lock file %s: %s", lock_key, str(e))

def lambda_handler(event, context):
    """
    Evento atteso:
    {
       "bucket": "nome-del-bucket",        // opzionale, se non presente si usa la variabile d'ambiente
       "lock_key": "nome-del-lockfile",      // opzionale, se non presente si usa la variabile d'ambiente
       "timestamp": "2025-02-23T12:34:56"     // opzionale: se non presente verrà usato il timestamp corrente UTC
    }
    """
    logger.info("Evento ricevuto: %s", json.dumps(event))
    
    # Recupera i parametri dall'evento oppure dalle variabili d'ambiente
    bucket = event.get('bucket', os.environ.get('S3_BUCKET'))
    lock_key = event.get('lock_key', os.environ.get('LOCK_KEY')) + '.lock'
    
    ts_str = event.get('timestamp')
    if ts_str:
        try:
            timestamp = datetime.datetime.fromisoformat(ts_str)
        except Exception as e:
            logger.error("Errore nella conversione del timestamp fornito: %s", str(e))
            timestamp = datetime.datetime.utcnow()
    else:
        timestamp = datetime.datetime.utcnow()
    
    update_lock_file(bucket, lock_key, timestamp)
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Lock file aggiornato con successo.',
            'lock_key': lock_key,
            'timestamp': timestamp.isoformat()
        })
    }