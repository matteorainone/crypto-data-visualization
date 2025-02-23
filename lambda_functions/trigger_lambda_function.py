import os
import json
import boto3
import logging
import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Client per S3 e Step Functions
s3_client = boto3.client('s3')
sf_client = boto3.client('stepfunctions')

# Variabili d'ambiente per il bucket
S3_BUCKET = os.environ.get('S3_BUCKET')

# Configurazione per BTC
BTC_FILE1 = os.environ.get('BTC_FILE1')
BTC_FILE2 = os.environ.get('BTC_FILE2')
BTC_STEP_FUNCTION_ARN = os.environ.get('BTC_STEP_FUNCTION_ARN')
BTC_LOCK_KEY = os.environ.get('BTC_LOCK_KEY')

# Configurazione per MONERO
MONERO_FILE1 = os.environ.get('MOENRO_FILE1')
MONERO_FILE2 = os.environ.get('MONERO_FILE2')
MONERO_STEP_FUNCTION_ARN = os.environ.get('MONERO_STEP_FUNCTION_ARN')
MONERO_LOCK_KEY = os.environ.get('MONERO_LOCK_KEY')

def file_exists(bucket, key):
    """
    Verifica se il file con chiave 'key' esiste nel bucket S3 specificato.
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as e:
        logger.info("Il file %s non è ancora presente: %s", key, str(e))
        return False

def get_last_modified(bucket, key):
    """
    Recupera il timestamp di ultima modifica del file.
    """
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        return response['LastModified']
    except Exception as e:
        logger.error("Errore nel recuperare LastModified per %s: %s", key, str(e))
        return None

def read_lock_timestamp(bucket, lock_key):
    """
    Legge il timestamp salvato nel file di lock, se presente.
    Il file di lock contiene una stringa in formato ISO.
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=lock_key)
        lock_timestamp_str = response['Body'].read().decode('utf-8').strip()
        # Converte la stringa in oggetto datetime
        lock_timestamp = datetime.datetime.fromisoformat(lock_timestamp_str)
        return lock_timestamp
    except s3_client.exceptions.NoSuchKey:
        # Il file di lock non esiste
        logger.info("Il file di lock %s non esiste.", lock_key)
        return None
    except Exception as e:
        logger.error("Errore nella lettura del file di lock %s: %s", lock_key, str(e))
        return None

def trigger_step_function(state_machine_arn):
    """
    Avvia la step function senza passare alcun input payload.
    """
    response = sf_client.start_execution(
        stateMachineArn=state_machine_arn
    )
    logger.info("Step function %s avviata con risposta: %s", state_machine_arn, response)
    return response

def process_crypto(crypto_name, file1, file2, step_function_arn, lock_key):
    """
    Funzione generica per gestire il controllo ed il trigger per una criptovaluta.
    """
    logger.info("Verifica per %s", crypto_name)
    # Controllo presenza dei file
    if file_exists(S3_BUCKET, file1) and file_exists(S3_BUCKET, file2):
        ts1 = get_last_modified(S3_BUCKET, file1)
        ts2 = get_last_modified(S3_BUCKET, file2)
        if ts1 is None or ts2 is None:
            logger.error("Impossibile recuperare i timestamp per i file di %s", crypto_name)
            return None

        # Consideriamo il timestamp più recente tra i due file
        new_update_ts = max(ts1, ts2)
        logger.info("Timestamp aggiornamento %s: %s", crypto_name, new_update_ts.isoformat())

        # Legge il lock file (se esistente)
        lock_ts = read_lock_timestamp(S3_BUCKET, lock_key)
        if lock_ts and new_update_ts <= lock_ts:
            logger.info("Per %s l'aggiornamento %s risulta già processato (lock: %s). Nessuna nuova esecuzione.",
                        crypto_name, new_update_ts.isoformat(), lock_ts.isoformat())
            return None

        # Avvia la Step Function e aggiorna il lock file
        logger.info("Avvio della pipeline ETL per %s", crypto_name)
        response = trigger_step_function(step_function_arn)
        return response
    else:
        logger.info("I file per %s non sono ancora completi.", crypto_name)
        return None

def lambda_handler(event, context):
    logger.info("Evento ricevuto: %s", json.dumps(event))

    executions = {}

    # Gestione per BTC
    exec_crypto1 = process_crypto(
        crypto_name="BTC",
        file1=BTC_FILE1,
        file2=BTC_FILE2,
        step_function_arn=BTC_STEP_FUNCTION_ARN,
        lock_key=BTC_LOCK_KEY
    )
    executions['BTC'] = exec_crypto1

    # Gestione per MONERO
    exec_crypto2 = process_crypto(
        crypto_name="MONERO",
        file1=MONERO_FILE1,
        file2=MONERO_FILE2,
        step_function_arn=MONERO_STEP_FUNCTION_ARN,
        lock_key=MONERO_LOCK_KEY
    )
    executions['MONERO'] = exec_crypto2

    return {
        'statusCode': 200,
        'body': json.dumps({
            "message": "Verifica e trigger delle step functions completata.",
            "executions": executions
        })
    }