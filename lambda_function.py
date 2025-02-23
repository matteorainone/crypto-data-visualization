import os
import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Client per S3 e Step Functions
s3_client = boto3.client('s3')
sf_client = boto3.client('stepfunctions')

# Variabili d'ambiente per il bucket
S3_BUCKET = os.environ.get('S3_BUCKET')

# Configurazione per la criptovaluta 1
BTC_FILE1 = os.environ.get('BTC_FILE1')
BTC_FILE2 = os.environ.get('BTC_FILE2')
BTC_STEP_FUNCTION_ARN = os.environ.get('BTC_STEP_FUNCTION_ARN')

# Configurazione per la criptovaluta 2
MONERO_FILE1 = os.environ.get('MOENRO_FILE1')
MONERO_FILE2 = os.environ.get('MONERO_FILE2')
MONERO_STEP_FUNCTION_ARN = os.environ.get('MONERO_STEP_FUNCTION_ARN')

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

def trigger_step_function(state_machine_arn):
    """
    Avvia la step function senza passare alcun input payload.
    """
    response = sf_client.start_execution(
        stateMachineArn=state_machine_arn
    )
    logger.info("Step function %s avviata con risposta: %s", state_machine_arn, response)
    return response

def lambda_handler(event, context):
    logger.info("Evento ricevuto: %s", json.dumps(event))
    
    triggered_executions = []

    # Verifica per BTC
    if file_exists(S3_BUCKET, BTC_FILE1) and file_exists(S3_BUCKET, BTC_FILE2):
        logger.info("Entrambi i file per BTC sono disponibili: %s e %s", BTC_FILE1, BTC_FILE2)
        input_payload = {
            "bucket": S3_BUCKET,
            "file1": BTC_FILE1,
            "file2": BTC_FILE2
        }
        response = trigger_step_function(BTC_STEP_FUNCTION_ARN)
        triggered_executions.append(response)
    else:
        logger.info("I file per Crypto 1 non sono ancora completi.")

    # Verifica per MONERO
    if file_exists(S3_BUCKET, MONERO_FILE1) and file_exists(S3_BUCKET, MONERO_FILE2):
        logger.info("Entrambi i file per MONERO sono disponibili: %s e %s", MONERO_FILE1, MONERO_FILE2)
        input_payload = {
            "bucket": S3_BUCKET,
            "file1": MONERO_FILE1,
            "file2": MONERO_FILE2
        }
        response = trigger_step_function(MONERO_STEP_FUNCTION_ARN)
        triggered_executions.append(response)
    else:
        logger.info("I file per MONERO non sono ancora completi.")

    return {
        'statusCode': 200,
        'body': json.dumps({
            "message": "Step functions avviate (se i file risultavano completi).",
            "executions": triggered_executions
        })
    }