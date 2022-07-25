#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Example Airflow DAG that show how to use various Dataproc
operators to manage a cluster and submit jobs.
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
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor



PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "capstone-356805")
CLUSTER_NAME = os.environ.get("GCP_DATAPROC_CLUSTER_NAME", "example-cluster")
REGION = os.environ.get("GCP_LOCATION", "europe-west1")
ZONE = os.environ.get("GCP_REGION", "europe-west1-b")
BUCKET = os.environ.get("GCP_DATAPROC_BUCKET", "dataproc-system-tests")
OUTPUT_FOLDER = "wordcount"
OUTPUT_PATH = f"gs://{BUCKET}/{OUTPUT_FOLDER}/"
PYSPARK_MAIN = os.environ.get("PYSPARK_MAIN", "hello_world.py")
PYSPARK_URI = f"gs://{BUCKET}/{PYSPARK_MAIN}"
SPARKR_MAIN = os.environ.get("SPARKR_MAIN", "hello_world.R")
SPARKR_URI = f"gs://{BUCKET}/{SPARKR_MAIN}"

# Cluster definition
# [START how_to_cloud_dataproc_create_cluster]

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-1",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-1",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
}

# [END how_to_cloud_dataproc_create_cluster]

# Cluster definition: Generating Cluster Config for DataprocCreateClusterOperator
# [START how_to_cloud_dataproc_create_cluster_generate_cluster_config]
path = "gs://goog-dataproc-initialization-actions-us-central1/python/pip-install.sh"

CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
    project_id="test",
    zone="us-central1-a",
    master_machine_type="n1-standard-1",
    worker_machine_type="n1-standard-1",
    num_workers=2,
    storage_bucket="test",
    init_actions_uris=[path],
    metadata={'PIP_PACKAGES': 'pyyaml requests pandas openpyxl'},
).make()

create_cluster_operator = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    cluster_name="test",
    project_id="test",
    region="us-central1",
    cluster_config=CLUSTER_GENERATOR_CONFIG,
)
# [END how_to_cloud_dataproc_create_cluster_generate_cluster_config]

# Update options
# [START how_to_cloud_dataproc_updatemask_cluster_operator]
CLUSTER_UPDATE = {
    "config": {"worker_config": {"num_instances": 3}, "secondary_worker_config": {"num_instances": 3}}
}
UPDATE_MASK = {
    "paths": ["config.worker_config.num_instances", "config.secondary_worker_config.num_instances"]
}
# [END how_to_cloud_dataproc_updatemask_cluster_operator]

TIMEOUT = {"seconds": 1 * 24 * 60 * 60}

# Jobs definitions

# [START how_to_cloud_dataproc_sparksql_config]
SPARK_SQL_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_sql_job": {"query_list": {"queries": ["SHOW DATABASES;"]}},
}
# [END how_to_cloud_dataproc_sparksql_config]

# [START how_to_cloud_dataproc_spark_config]
SPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
        "main_class": "org.apache.spark.examples.SparkPi",
    },
}
# [END how_to_cloud_dataproc_spark_config]



PYSPARK_URI="gs://bucket-356805/moviereviews_sparkapp.py"
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
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )
    # [END how_to_cloud_dataproc_create_cluster_operator]


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

    create_cluster >> pyspark_task >> delete_cluster

