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

# Postgres constants
POSTGRES_CONN_ID = "postgres_local"
POSTGRES_TABLE_NAME = "user_purchase"


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

dag_psql = DAG(
    dag_id = "user_purchase_setup_dag",
    default_args=args,
    # schedule_interval='0 0 * * *',
    schedule_interval='@once',	
    dagrun_timeout=timedelta(minutes=60),
    description='use case of psql operator in airflow',
    start_date = airflow.utils.dates.days_ago(1)
)


create_table_sql_query = """ 
create schema dbschema;
ALTER SCHEMA dbschema OWNER TO dbuser;
CREATE TABLE dbschema.user_purchase (
   invoice_number varchar(10),
   stock_code varchar(20),
   detail varchar(1000),
   quantity int,
   invoice_date timestamp,
   unit_price numeric(8,3),
   customer_id int,
   country varchar(20)
)
;
"""


delete_table_sql_query="""
drop schema if exists dbschema cascade;
drop table if exists dbschema.user_purchase ;
"""

insert_data_sql_query = """
COPY dbschema.user_purchase(invoice_number, stock_code, detail, quantity, invoice_date, unit_price, customer_id, country)
FROM '/data/var/incoming/capstone/user_purchase.csv'
DELIMITER ','
CSV HEADER;
"""


delete_table = PostgresOperator(
    sql = delete_table_sql_query,
    task_id = "delete_table_task",
    postgres_conn_id = "postgres_local",
    dag = dag_psql
    )

create_table = PostgresOperator(
    sql = create_table_sql_query,
    task_id = "create_table_task",
    postgres_conn_id = "postgres_local",
    dag = dag_psql
    )

insert_data = PostgresOperator(
    sql = insert_data_sql_query,
    task_id = "insert_data_task",
    postgres_conn_id = "postgres_local",
    dag = dag_psql
    )
    
ingest_data = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest_data_from_gcs,
        op_kwargs={
            "gcp_conn_id": GCP_CONN_ID,
            "postgres_conn_id": POSTGRES_CONN_ID,
            "gcs_bucket": GCS_BUCKET_NAME,
            "gcs_object": GCS_KEY_NAME,
            "postgres_table": POSTGRES_TABLE_NAME,
        },
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )    



delete_table >> create_table  >>  ingest_data 

if __name__ == "__main__":
        dag_psql.cli()
