from datetime import timedelta
from airflow import DAG
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from airflow.utils.dates import days_ago
from datetime import datetime
import datetime
import sqlite3

# Local imports

from moviereviews_etl import run_moviereviews_etl 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'moviereviews_dag',
    default_args=default_args,
    description='ETL process for movie reviews',
    schedule_interval=timedelta(days=1),
)

spark_submit_local = SparkSubmitOperator(
		application ='moviewreviews_sparkapp.py' ,
		conn_id= 'spark_local', 
		task_id='spark_submit_task', 
		dag=dag_spark
		)
        
run_etl = PythonOperator(
    task_id='whole_moviereviews_etl',
    python_callable=run_moviereviews_etl,
    dag=dag,
)

spark_submit_local

