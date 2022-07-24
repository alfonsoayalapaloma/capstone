
import os
import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
   DataprocCreateClusterOperator,
   DataprocDeleteClusterOperator,
   DataprocSubmitJobOperator
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.utils.dates import days_ago

PROJECT_ID = "capstone-356805"
CLUSTER_NAME =  "dataproc-cluster-356805"
REGION = "us-central1"
ZONE = "us-central1-a" 
GKE_CLUSTER_NAME="airflow-gke-data-bootcamp"
PYSPARK_URI = "gs://bucket-356805/moviereviews_sparkapp.py"
STAGING_BUCKET="bucket-stage-356805"

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_dag_args = {
    'start_date': YESTERDAY,
}

# Cluster definition
# [START how_to_cloud_dataproc_create_cluster]


CLUSTER_CONFIG = {
   "master_config": {
       "num_instances": 1,
       "machine_type_uri": "n1-standard-4",
       "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
   },
   "worker_config": {
       "num_instances": 2,
       "machine_type_uri": "n1-standard-4",
       "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
   },
}


VIRTUAL_CLUSTER_CONFIG = {
    "kubernetes_cluster_config": {
        "gke_cluster_config": {
            "gke_cluster_target": f"projects/{PROJECT_ID}/locations/{REGION}/clusters/{GKE_CLUSTER_NAME}",
            "node_pool_target": [
                {
                    "node_pool": f"projects/{PROJECT_ID}/locations/{REGION}/clusters/{GKE_CLUSTER_NAME}/nodePools/dp",  # noqa
                    "roles": ["DEFAULT"],
                }
            ],
        },
        "kubernetes_software_config": {"component_version": {"SPARK": b'3'}},
    },
    "staging_bucket": "{STAGING_BUCKET}",
}



with models.DAG(
   "moviereviews_spark_dag",
   schedule_interval=datetime.timedelta(days=1),
   default_args=default_dag_args) as dag:

   # [START how_to_cloud_dataproc_create_cluster_operator]
   create_cluster = DataprocCreateClusterOperator(
       task_id="create_cluster_in_gke",
       project_id=PROJECT_ID,
       region=REGION,
       cluster_name=CLUSTER_NAME,
       virtual_cluster_config=VIRTUAL_CLUSTER_CONFIG,
   )

   PYSPARK_JOB = {
   "reference": {"project_id": PROJECT_ID},
   "placement": {"cluster_name": CLUSTER_NAME},
   "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
   }

   pyspark_task = DataprocSubmitJobOperator(
       task_id="pyspark_task", job=PYSPARK_JOB, location=REGION, project_id=PROJECT_ID
   )
   
   delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
   )

create_cluster >>  pyspark_task >> delete_cluster 

if __name__ == "__main__":
        dag.cli()

