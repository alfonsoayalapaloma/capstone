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
PYSPARK_URI="gs://bucket-356805/moviereviews_sparkapp.py"


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

with models.DAG(
    "gcp_dataproc",
    schedule_interval='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    # [START how_to_cloud_dataproc_create_cluster_operator]
    #create_cluster = DataprocCreateClusterOperator(
    #    task_id="create_cluster",
    #    project_id=PROJECT_ID,
    #    cluster_config=CLUSTER_CONFIG,
    #    region=REGION,
    #    cluster_name=CLUSTER_NAME,
    #)
    # [END how_to_cloud_dataproc_create_cluster_operator]

    #gcs_delete_temp = GCSDeleteObjectsOperator(
    #    task_id="delete_files", bucket_name=GS_BUCKET, objects=[GS_OUTPUT_FILE]
    #)

    def delete_obj():
         hook = GCSHook()
         hook.delete(bucket_name=GS_BUCKET, object_name=GS_OUTPUT_FILE)

    gcs_delete_temp = python.PythonOperator(
            task_id='delete_gcs_obj',
            provide_context=True,
            python_callable=delete_obj,
            )


    # [START how_to_cloud_dataproc_submit_job_to_cluster_operator]
    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task", job=PYSPARK_JOB, region=REGION, project_id=PROJECT_ID
    )
    # [END how_to_cloud_dataproc_submit_job_to_cluster_operator]


    # [START how_to_cloud_dataproc_delete_cluster_operator]
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", project_id=PROJECT_ID, cluster_name=CLUSTER_NAME, region=REGION
    )
    # [END how_to_cloud_dataproc_delete_cluster_operator]

    #create_cluster >> gcs_delete_temp >> pyspark_task >> delete_cluster
    gcs_delete_temp >> pyspark_task >> delete_cluster

