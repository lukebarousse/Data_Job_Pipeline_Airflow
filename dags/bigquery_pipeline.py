"""
An operations workflow to clean up BigQuery table making fact and dimension tables
"""
import os
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning) # stop getting Pandas FutureWarning's

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from config import config  # contains secret keys in config.py
from google.cloud import bigquery
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator, DataprocDeleteClusterOperator, DataprocCreateClusterOperator, DataprocSubmitJobOperator
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import timedelta

from youtube.google_oauth import update_video
from modules.job_title import transform_job_title


# 'False' DAG is ready for operation; 'True' DAG only runs 'start' DummyOperator
TESTING_DAG = False
# Minutes to sleep on an error
ERROR_SLEEP_MIN = 5 
# Who is listed as the owner of this DAG in the Airflow Web Server
DAG_OWNER_NAME = "airflow"
# List of email address to send email alerts to if this job fails
ALERT_EMAIL_ADDRESSES = ['luke@lukebarousse.com']
START_DATE = airflow.utils.dates.days_ago(1)

default_args = {
    'owner': DAG_OWNER_NAME,
    'depends_on_past': False,
    'start_date': START_DATE, 
    'email': ALERT_EMAIL_ADDRESSES,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1, 
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2022, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

# Dataproc cluster variables
PROJECT_ID = 'job-listings-366015'
REGION = 'us-central1'
ZONE = 'us-central1-a'
CLUSTER_NAME = 'spark-cluster'
BUCKET_NAME = 'dataproc-cluster-gsearch'

CLUSTER_CONFIG = ClusterGenerator(
    task_id='start_cluster',
    gcp_conn_id='google_cloud_default',
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    zone=ZONE,
    storage_bucket=BUCKET_NAME,
    num_workers=2,
    master_machine_type='n2-standard-2',
    worker_machine_type='n2-standard-2',
    image_version='1.5-debian10',
    optional_components=['ANACONDA', 'JUPYTER'],
    properties={'spark:spark.jars.packages': 'com.johnsnowlabs.nlp:spark-nlp_2.12:4.2.6,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.27.1'},
    metadata={'PIP_PACKAGES': 'spark-nlp spark-nlp-display'},
    init_actions_uris=['gs://goog-dataproc-initialization-actions-us-central1/python/pip-install.sh'],
    auto_delete_ttl=60*60*4, # 4 hours
    enable_component_gateway=True
).make()

dag = DAG(
    'bigquery_pipeline', 
    description='Execute BigQuery & Dataproc jobs for data pipeline',
    default_args=default_args, 
    schedule_interval='0 10 * * *', # want to run following serpapi_bigquery dag... best practice is to combine don't want to combine and make script so big
    catchup=False,
    tags=['data-pipeline-dag'],
    max_active_tasks = 3
)

with dag:

    start = DummyOperator(
        task_id='start',
        dag=dag)
    
    if not TESTING_DAG:

        # Create fact table from JSON data from SerpApi
        fact_table_build = BigQueryInsertJobOperator(
            task_id='fact_table_build',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": 'sql/fact_build.sql',
                    "useLegacySql": False
                }
            },
            dag=dag
        )

        # Start up dataproc cluster with spark-nlp and spark-bigquery dependencies
        # NOTE: CLI commands in notes under 'Dataproc' section
        start_cluster = DataprocCreateClusterOperator(
            task_id='start_cluster',
            cluster_name=CLUSTER_NAME,
            project_id=PROJECT_ID,
            region=REGION,
            cluster_config=CLUSTER_CONFIG,
            dag=dag
        )

        # Create (and/or replace) salary table
        python_file_salary = 'gs://dataproc-cluster-gsearch/notebooks/jupyter/salary_table.py'
        SALARY_TABLE = {
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {"main_python_file_uri": python_file_salary},
            }

        salary_table = DataprocSubmitJobOperator(
            task_id='salary_table',
            job=SALARY_TABLE,
            project_id=PROJECT_ID,
            region=REGION,
            dag=dag
        )

        # Append to skill table
        python_file_skill = 'gs://dataproc-cluster-gsearch/notebooks/jupyter/skill_table.py'
        SKILL_TABLE = {
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {"main_python_file_uri": python_file_skill},
            }

        skill_table = DataprocSubmitJobOperator(
            task_id='skill_table',
            job=SKILL_TABLE,
            project_id=PROJECT_ID,
            region=REGION,
            dag=dag
        )

        # Shut down dataproc cluster
        stop_cluster = DataprocDeleteClusterOperator(
            task_id='stop_cluster',
            project_id=PROJECT_ID,
            cluster_name=CLUSTER_NAME,
            region=REGION,
            dag=dag
        )

        # Transform job title using BART
        transform_job = PythonOperator(
        task_id='transform_job_title',
        python_callable=transform_job_title,
        dag=dag
        )

        # Combine fact table with dimension table
        wide_table_build = BigQueryInsertJobOperator(
            task_id='wide_table_build',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": 'sql/wide_build.sql',
                    "useLegacySql": False
                }
            },
            dag=dag
        )

        # Cache common BigQuery queries in CSV files
        cache_csv = BigQueryInsertJobOperator(
            task_id='cache_csv',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": 'sql/cache_csv.sql',
                    "useLegacySql": False
                }
            },
            dag=dag
        )

        # Update video title in YouTube with number of job listings
        update_video_title = PythonOperator(
        task_id='update_video_title',
        python_callable=update_video,
        dag=dag
        )

        # Create public dataset w/ No duplicates (for ChatGPT course)
        public_table_build = BigQueryInsertJobOperator(
            task_id='public_table_build',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": 'sql/public_build.sql',
                    "useLegacySql": False
                }
            },
            dag=dag
        )


        start >> fact_table_build >> start_cluster >> salary_table >> skill_table >> stop_cluster >> transform_job >> wide_table_build >> cache_csv >> update_video_title >> public_table_build
