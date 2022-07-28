"""
Airflow DAG to submit pyspark processes to a Dataproc cluster
"""

import os
from datetime import datetime
from uuid import uuid4

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateBatchOperator,
    DataprocCreateClusterOperator,
    DataprocCreateWorkflowTemplateOperator,
    DataprocDeleteBatchOperator,
    DataprocDeleteClusterOperator,
    DataprocGetBatchOperator,
    DataprocInstantiateInlineWorkflowTemplateOperator,
    DataprocInstantiateWorkflowTemplateOperator,
    DataprocListBatchesOperator,
    DataprocSubmitJobOperator,
    DataprocUpdateClusterOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSBucketCreateAclEntryOperator,
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSDeleteObjectsOperator,
    GCSFileTransformOperator,
    GCSListObjectsOperator,
    GCSObjectCreateAclEntryOperator,
)
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators import python

from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
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
    BigQueryExecuteQueryOperator,
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
from airflow.contrib.operators.gcs_to_bq import  GoogleCloudStorageToBigQueryOperator


PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "capstone-356805")
CLUSTER_NAME = os.environ.get("GCP_DATAPROC_CLUSTER_NAME", "capstone-356805-cluster")
REGION = os.environ.get("GCP_LOCATION", "europe-west1")
ZONE = os.environ.get("GCP_REGION", "europe-west1-b")
BUCKET = os.environ.get("GCP_DATAPROC_BUCKET", "dataproc-system-tests")
OUTPUT_FOLDER = "wordcount"
OUTPUT_PATH = f"gs://{BUCKET}/{OUTPUT_FOLDER}/"
PYSPARK_MAIN = os.environ.get("PYSPARK_MAIN", "hello_world.py")
PYSPARK_URI = f"gs://{BUCKET}/{PYSPARK_MAIN}"
SPARKR_MAIN = os.environ.get("SPARKR_MAIN", "hello_world.R")
SPARKR_URI = f"gs://{BUCKET}/{SPARKR_MAIN}"


GS_OUTPUT_FILE="moviereview"
GS_BUCKET="bucket-stage-356805"
PYSPARK_URI=     "gs://bucket-356805/moviereviews_sparkapp.py"
PYSPARK_URI_LOGS="gs://bucket-356805/logreviews_sparkapp.py"
GCP_CONN_ID = "google_cloud_default"


# Cluster definition
# [START how_to_cloud_dataproc_create_cluster]

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    },
}

# [END how_to_cloud_dataproc_create_cluster]


TIMEOUT = {"seconds": 1 * 24 * 60 * 60}

# Jobs definitions

# [START how_to_cloud_dataproc_pyspark_config]
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}
# [END how_to_cloud_dataproc_pyspark_config]
# [START how_to_cloud_dataproc_pyspark_config]
PYSPARK_JOB_LOGS = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI_LOGS},
}
# [END how_to_cloud_dataproc_pyspark_config]

SQL_CREATE_DIMS="""
drop table IF EXISTS movieds.review_logs;
create table movieds.review_logs as 
SELECT log_id, PARSE_DATE('%m-%d-%Y',  log_date_str) as log_date, device, os, 
location,
CASE WHEN os ="Microsoft Windows"  THEN 'Microsoft Edge'
WHEN os="Linux" THEN 'Firefox'
WHEN os="Apple MacOS" then 'Safari'
WHEN os="Google Android" then 'Chrome'
WHEN os="Apple iOS" then 'Safari'
ELSE ''
END AS browser,
 ip, phone_number 
FROM `capstone-356805.movieds.stage_review_logs`;
--------------------------------------
drop table IF EXISTS movieds.dim_devices;
CREATE TABLE movieds.dim_devices (
	   id_dim_devices INTEGER,
	   device STRING
);
insert into movieds.dim_devices(device,id_dim_devices)
select   device, 
RANK() OVER (ORDER BY device ASC) AS id
 from (
select  device , count(*) as qty
from movieds.review_logs 
group by device
 )a;
--------------------------------------
drop table IF EXISTS movieds.dim_os;
CREATE TABLE movieds.dim_os (
	   id_dim_os INTEGER,
	   os STRING
);
insert into movieds.dim_os(os,id_dim_os)
select   os, 
RANK() OVER (ORDER BY os ASC) AS id
 from (
select   os,  count(*) as qty
from movieds.review_logs 
group by os
 )a;
--------------------------------------
drop table IF EXISTS movieds.dim_location;
CREATE TABLE movieds.dim_location (
	   id_dim_location INTEGER,
	   location STRING
);
insert into movieds.dim_location(location,id_dim_location)
select   location, 
RANK() OVER (ORDER BY location ASC) AS id
 from (
select   location,  count(*) as qty
from movieds.review_logs 
group by location
 )a;
----------------------------------------
drop table IF EXISTS movieds.dim_browser;
CREATE TABLE movieds.dim_browser (
	   id_dim_browser INTEGER,
	   browser STRING
);
insert into movieds.dim_browser(browser,id_dim_browser)
select   browser, 
RANK() OVER (ORDER BY browser ASC) AS id
 from (
select   browser,  count(*) as qty
from movieds.review_logs 
group by browser
 )a;

truncate table movieds.dim_date;
insert into movieds.dim_date(log_date,day,month,year,season, id_dim_date)
select log_date, cast(day as STRING), cast(month as STRING) ,cast(year as STRING) , 
CASE WHEN month between 1  and 2 THEN 'winter'
WHEN month between 3 and 5 THEN 'sprint'
WHEN month between 6 and 8 then 'summer'
WHEN month between 9 and 11 then 'autum'
ELSE 'winter'
END AS season,
RANK() OVER (ORDER BY log_date ASC) AS id
from (
select   log_date, 
extract(day from log_date) as day ,
extract(month from log_date)as month,
extract(year from log_date)as year,
 count(*) as qty
from movieds.review_logs 
group by log_date
)a; 
--------------------

"""

SQL_CREATE_FACT="""
drop table IF EXISTS movieds.tmp_customer_agg;
create table movieds.tmp_customer_agg as 
SELECT p.customer_id, 
SUM(p.quantity * p.unit_price) as amount_spent,
SUM(r.is_positive) as review_score,
COUNT(r.review_id) as review_count
 FROM 
 `movieds.classified_movie_review` r 
join  movieds.user_purchase p on (p.customer_id=r.customer_id)
group by p.customer_id
order by p.customer_id;
---------------------
drop table IF EXISTS movieds.tmp_logs_per_customer;
create table movieds.tmp_logs_per_customer as 
select r.customer_id, r.review_id, d.id_dim_devices, c.id_dim_location ,o.id_dim_os, b.id_dim_browser, 
 l.device, l.location, l.os, l.browser ,l.log_date, t.id_dim_date   
from movieds.review_logs l 
join `movieds.classified_movie_review` r on (r.review_id=l.log_id)
left join movieds.dim_devices d on (d.device=l.device)
left join movieds.dim_location c on (c.location=l.location)
left join movieds.dim_os o on (o.os=l.os)
left join movieds.dim_browser b on (b.browser=l.browser)
left join movieds.dim_date t on (t.log_date=l.log_date) ;
---------------------
drop table IF EXISTS movieds.fact_movie_analytics ;
create table movieds.fact_movie_analytics  as 
select l.customer_id, l.id_dim_devices, l.id_dim_location, l.id_dim_os, l.id_dim_browser,
c.amount_spent, c.review_score, c.review_count ,CURRENT_TIMESTAMP() as insert_date, l.id_dim_date, l.log_date as review_date, l.review_id  
from movieds.tmp_logs_per_customer  l 
left join movieds.tmp_customer_agg c on (c.customer_id = l.customer_id )
order by 1;

"""


SQL_CLEANUP ="""
--clean up 
drop table if exists movieds.tmp_customer_agg;
drop table if exists movieds.tmp_logs_per_customer;
drop table if exists movieds.tmp_logs_per_user; 
drop table if exists movieds.stage_review_logs;
"""

with models.DAG(
        "states_dag",
    schedule_interval='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    # Postgres constants
    POSTGRES_CONN_ID = "postgres_local"
    POSTGRES_TABLE_NAME = "user_purchase"

    # Bigquery
    DATASET_NAME="movieds"
	   
    # Stage file 
    GCS_PURCHASE_STAGE_FILE="user_purchase_pro.csv"
    GCS_BUCKET_STAGE_NAME="bucket-stage-356805"	   
    GCS_STAGE_PURCHASE="user_purchase_pro.csv"
    GCS_STAGE_REVIEW="moviereview/part*"
    GCS_STAGE_LOGS="logreview/part*"
    GCS_STAGE_STATES="states.csv"
	   
    create_bq_states = BigQueryCreateExternalTableOperator(
        dag=dag,
        task_id="create_bq_state",
        destination_project_dataset_table=f"{DATASET_NAME}.state",
        bucket=GCS_BUCKET_STAGE_NAME,
        source_objects=[GCS_STAGE_STATES],
        quote_character="^",
        field_delimiter=",",
        schema_fields=[
            {"name": "state", "type": "STRING", "mode": "NULLABLE"},
            {"name": "latitude",  "type": "DECIMAL", "mode": "NULLABLE"},
            {"name": "longitude", "type": "DECIMAL", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        ],
    )
    csv_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id='csv_to_bigquery',
        google_cloud_storage_conn_id=GCS_CONN_ID,
        bucket=airflow_bucket,
        source_objects=[GCS_STAGE_STATES],
        skip_leading_rows=1,
        bigquery_conn_id=GCS_CONN_ID,
        destination_project_dataset_table='{}.{}.{}'.format(project, schema, table),
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        schema_update_options=['ALLOW_FIELD_RELAXATION', 'ALLOW_FIELD_ADDITION'],
        autodetect=True,
        dag=dag
    )

    create_bq_states

