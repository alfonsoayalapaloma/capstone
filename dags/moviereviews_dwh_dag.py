import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from contextlib import closing
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryDeleteTableOperator,
    BigQueryGetDatasetTablesOperator,
    BigQueryUpdateDatasetOperator,
    BigQueryUpdateTableOperator,
    BigQueryUpdateTableSchemaOperator,
    BigQueryUpsertTableOperator,
    BigQueryCreateExternalTableOperator,    
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.sql import BranchSQLOperator
import logging
from tempfile import NamedTemporaryFile

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule



args={'owner': 'airflow'}

default_args = {
    'owner': 'airflow',    
    #'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    #'email': ['airflow@example.com'],
    #'email_on_failure': False,
    # 'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}



# General constants
DAG_ID = "gcp_database_ingestion_workflow"
STABILITY_STATE = "unstable"
CLOUD_PROVIDER = "gcp"

# GCP constants
GCP_CONN_ID = "google_cloud_default"
GCS_BUCKET_NAME = "bucket-356805"
GCS_KEY_NAME = "user_purchase.csv"


GCS_BUCKET_STAGE_NAME = "bucket-stage-356805"
#GCS_KEY_STAGE_NAME = "movie_review_pro.csv"
GCS_KEY_STAGE_NAME = "moviereview"

# Postgres constants
POSTGRES_CONN_ID = "postgres_local"
POSTGRES_TABLE_NAME = "user_purchase"

# Bigquery
DATASET_NAME="movieds"


dag = DAG(
    dag_id = "moviereviews_dwh_dag",
    default_args=args,
    # schedule_interval='0 0 * * *',
    schedule_interval='@once',	
    dagrun_timeout=timedelta(minutes=60),
    description='',
    start_date = airflow.utils.dates.days_ago(1)
)

start_workflow = DummyOperator(task_id="start_workflow",dag=dag)
delete_staging = DummyOperator(task_id="delete_staging",dag=dag)
create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create_dataset", dataset_id=DATASET_NAME, dag=dag)

create_bq_table = BigQueryCreateExternalTableOperator(
    dag=dag,
    task_id="create_bq_table",
    destination_project_dataset_table=f"{DATASET_NAME}.classified_movie_review",
    bucket=GCS_BUCKET_STAGE_NAME,
    source_objects=[GCS_KEY_STAGE_NAME],
    quote_character="^",
    field_delimiter=",",
    schema_fields=[
        {"name": "customer_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "is_positive", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "review_id", "type": "INTEGER", "mode": "NULLABLE"},
    ],
)



start_workflow >> delete_staging >> create_dataset >> create_bq_table 

if __name__ == "__main__":
        dag.cli()
