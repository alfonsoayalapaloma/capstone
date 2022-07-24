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
    dag_id = "log_reviews_staging_dag",
    default_args=args,
    # schedule_interval='0 0 * * *',
    schedule_interval='@once',	
    dagrun_timeout=timedelta(minutes=60),
    description='reads log file and writes it to staging',
    start_date = airflow.utils.dates.days_ago(1)
)


import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession, functions

def spark_staging():
    spark = SparkSession \
        .builder \
        .appName("log_reviews_staging") \
        .config("spark.jars.packages",
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0,com.microsoft.sqlserver:mssql-jdbc:8.4.1.jre8") \
        .config("spark.driver.maxResultSize", "8g") \
        .config("spark.network.timeout", 10000000) \
        .config("spark.executor.heartbeatInterval", 10000000) \
        .config("spark.storage.blockManagerSlaveTimeoutMs", 10000000) \
        .config("spark.executor.memory", "10g") \
        .master("spark://192.168.0.1:7077") \
        .getOrCreate()

    inicio = datetime.now()
    print(inicio)
    # criação de data frame com extração de dados
    df = spark.read.format("jdbc") \
        .option("url", "jdbc:sqlserver://127.0.0.1:1433;databaseName=Teste") \
        .option("user", 'Teste') \
        .option("password", 'teste') \
        .option("numPartitions", 100) \
        .option("partitionColumn", "Id") \
        .option("lowerBound", 1) \
        .option("upperBound", 488777675) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("dbtable", "(select Id, DataVencimento AS Vencimento, TipoCod AS CodigoTipoDocumento, cast(recsld as FLOAT) AS Saldo from DocumentoPagar \
         where TipoCod in ('200','17') and RecPag = 'A') T") \
        .load()
    # agrupamento de dados e agregação de valores
    group = df.select("CodigoTipoDocumento", "Vencimento", "Saldo") \
        .groupby(["CodigoTipoDocumento", "Vencimento"]).agg(functions.sum("Saldo").alias("Saldo"))
    # carregamento de dados para dentro da base mongo
    group.write.format("com.mongodb.spark.sql.DefaultSource") \
        .mode("overwrite") \
        .option("database", "Financeiro") \
        .option("collection", "Fact_DocumentoPagar") \
        .save()
    termino = datetime.now()
    print(termino)
    print(termino - inicio)

with airflow.DAG('log_reviews_staging_dag',
                  default_args=default_args,
                  schedule_interval='0 1 * * *') as dag:
    task_staging = PythonOperator(
        task_id='spark_staging',
        python_callable=spark_staging
    )

 

if __name__ == "__main__":
        dag.cli()
