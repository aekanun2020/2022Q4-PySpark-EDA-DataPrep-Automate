import os
import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
   DataprocCreateClusterOperator,
   DataprocSubmitJobOperator
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.utils.dates import days_ago

PROJECT_ID = "polar-cyclist-364308"
CLUSTER_NAME =  "aekanun-wordcount-cluster"
REGION = "us-central1"
ZONE = "us-central1-a"
PYSPARK_URI = "gs://us-central1-myfirstpipeline-9f9cc53e-bucket/Pyspark-WordCount-1.py"

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

with models.DAG(
   "dataproc",

   schedule_interval=datetime.timedelta(days=1),
   default_args=default_dag_args) as dag:

   # [START how_to_cloud_dataproc_create_cluster_operator]
   create_cluster = DataprocCreateClusterOperator(
       task_id="create_cluster",
       project_id=PROJECT_ID,
       cluster_config=CLUSTER_CONFIG,
       region=REGION,
       cluster_name=CLUSTER_NAME,
   )

   PYSPARK_JOB = {
   "reference": {"project_id": PROJECT_ID},
   "placement": {"cluster_name": CLUSTER_NAME},
   "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
   }

   pyspark_task = DataprocSubmitJobOperator(
       task_id="pyspark_task", job=PYSPARK_JOB, region=REGION, project_id=PROJECT_ID
   )

   create_cluster >>  pyspark_task
