from datetime import datetime
import logging

import boto3
import psycopg2
import requests

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# Settingg up logging
logger = logging.getLogger(__name__)

def fetch_and_upload_to_s3():
    """
    Getting JSON from API and uploading to S3
    """
    # For production use Airflow Variables
    api_url = 'https://run.mocky.io/v3/b08dd72b-9b40-40a4-bcfb-d77234345d70'
    #api_url = Variable.get('PATH_TO_YOUR_API')
    bucket_name = 'greenflow'
    #bucket_name = Variable.get('BUCKET_NAME')
    object_name = 'api-json'
    #object_name = Variable.get('OBJECT_NAME')

    try:
        logger.info("Fetching data from API...")
        response = requests.get(api_url)
        logger.info("Data fetched successfully from API.")

        session= boto3.session.Session()
        s3 = session.client(
            service_name='s3',
            endpoint_url='https://storage.yandexcloud.net'
        )

        # Uploading JSON to S3
        logger.info(f"Uploading data to S3 bucket {bucket_name} with object name {object_name}.")
        s3.put_object(Bucket=bucket_name, Key=object_name, Body=response.text, StorageClass='STANDARD')
        logger.info("Data has been successfully uploaded to S3.")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

def run_greenplum_transformation():
    """
    This function receives JSON from S3 as from an external table,
    then makes a minus sign and loads it into the resulting table
    """
    connection_credentials = Variable.get('GREENPLUM_CONN_STR')

    try:
        logger.info("Connecting to Greenplum database...")
        with psycopg2.connect(connection_credentials) as connection:
            q = connection.cursor()
            q.execute("""
                INSERT INTO result_table (key, value)                
                WITH s3_data AS (
                    SELECT jsonb_each(s3.json_data) AS (key, value)
                    FROM external_s3_table s3
                ),
                gp_data AS (
                    SELECT jsonb_each(gp.json_data) AS (key, value)
                    FROM greenplum_table gp
                )
                SELECT s3.key, s3.value
                FROM s3_data s3
                LEFT JOIN gp_data gp ON s3.key = gp.key AND s3.value = gp.value
                WHERE gp.key IS NULL;
            """)

            connection.commit()
            logger.info("Data has been inserted into the resulting table.")
    
    except Exception as e:
        logger.error(f"An error occurred during the transformation: {e}")
        raise

# DAG creation
with DAG('api_to_s3_and_greenplum', start_date=datetime(2024, 1, 1), schedule_interval='@daily') as dag:

    fetch_and_upload_to_s3_task = PythonOperator(
        task_id='fetch_and_upload_to_s3',
        python_callable=fetch_and_upload_to_s3
    )

    run_greenplum_transformation_task = PythonOperator(
        task_id='run_greenplum_transformation',
        python_callable=run_greenplum_transformation,
        provide_context=True
    )

    fetch_and_upload_to_s3_task >> run_greenplum_transformation_task
