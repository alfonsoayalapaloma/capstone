import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from contextlib import closing


from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.sql import BranchSQLOperator
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
GCS_KEY_STAGE_NAME = "user_purchase_pro.csv"

# Postgres constants
POSTGRES_CONN_ID = "postgres_local"
POSTGRES_TABLE_NAME = "user_purchase"

# Bigquery
DATASET_NAME="movieds"


def ingest_data_from_gcs(
    gcs_bucket: str,
    gcs_object: str,
    postgres_table: str,
    gcp_conn_id: str = "google_cloud_default",
    postgres_conn_id: str = "postgres_default",
):
    """Ingest data from an GCS location into a postgres table.
    Args:
        gcs_bucket (str): Name of the bucket.
        gcs_object (str): Name of the object.
        postgres_table (str): Name of the postgres table.
        gcp_conn_id (str): Name of the Google Cloud connection ID.
        postgres_conn_id (str): Name of the postgres connection ID.
    """
    import tempfile
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    psql_hook = PostgresHook(postgres_conn_id)
    with tempfile.NamedTemporaryFile() as tmp:
        gcs_hook.download(
            bucket_name=gcs_bucket, object_name=gcs_object, filename=tmp.name
        )
        copy_sql = """
           COPY dbschema.user_purchase FROM stdin WITH CSV HEADER
           DELIMITER as ','
           """
        filename=tmp.name    
        with open(filename, 'r+') as file:
            with closing(psql_hook.get_conn()) as conn:
                with closing(conn.cursor()) as cur:
                    cur.copy_expert(copy_sql, file)
                    file.truncate(file.tell())
                    conn.commit()

dag = DAG(
    dag_id = "user_purchase_dag",
    default_args=args,
    # schedule_interval='0 0 * * *',
    schedule_interval='@once',	
    dagrun_timeout=timedelta(minutes=60),
    description='use case of psql operator in airflow',
    start_date = airflow.utils.dates.days_ago(1)
)



def copy_to_gcs(copy_sql, file_name, bucket_name):
    gcs_hook = GoogleCloudStorageHook(GCP_CONN_ID)
    pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID)

    with NamedTemporaryFile(suffix=".csv") as temp_file:
        temp_name = temp_file.name

        logging.info("Exporting query to file '%s'", temp_name)
        pg_hook.copy_expert(copy_sql, filename=temp_name)

        logging.info("Uploading to %s/%s", bucket_name, file_name)
        gcs_hook.upload(bucket_name, file_name, temp_name)


export_pg_table = PythonOperator(
        dag=dag,
        task_id="copy_to_gcs",
        python_callable=copy_to_gcs,
        op_kwargs={
            "copy_sql": "SELECT * FROM dbschema.user_purchase",
            "file_name": "user_purchase_pro.csv",
            "bucket_name": GCS_BUCKET_STAGE_NAME 
            }
        )
start_workflow = DummyOperator(task_id="start_workflow",dag=dag)
create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create_dataset", dataset_id=DATASET_NAME, dag=dag)

create_bq_table = BigQueryCreateExternalTableOperator(
    dag=dag,
    task_id="create_bq_table",
    destination_project_dataset_table=f"{DATASET_NAME}.user_purchase",
    bucket=GCS_BUCKET_STAGE_NAME,
    source_objects=[GCS_KEY_STAGE_NAME],
    schema_fields=[
        {"name": "invoice_number", "type": "STRING", "mode": "REQUIRED"},
        {"name": "stock_code", "type": "STRING", "mode": "REQUIRED"},
        {"name": "detail", "type": "STRING", "mode": "REQUIRED"},
        {"name": "quantity", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "invoice_date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "unit_price", "type": "NUMERIC", "mode": "REQUIRED"},
        {"name": "customer_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "country", "type": "STRING", "mode": "NULLABLE"},
    ],
)



start_workflow >> export_pg_table >> create_dataset >> create_bq_table 

if __name__ == "__main__":
        dag.cli()
