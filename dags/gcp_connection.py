"""
An initial workflow to enter in the conn_id for google cloud from the service account JSON
file (i.e. GOOGLE_APPLICATION_CREDENTIALS environment variable specified in docker-compose.yaml)
"""
import os
import airflow
from airflow import DAG, settings
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Connection

import json

DAG_OWNER_NAME = "airflow"
# List of email address to send email alerts to if this job fails
ALERT_EMAIL_ADDRESSES = ['luke@lukebarousse.com']
START_DATE = airflow.utils.dates.days_ago(1)

default_args = {
    "owner": DAG_OWNER_NAME,
    "depends_on_past": False,
    "start_date": START_DATE,
    "email": ALERT_EMAIL_ADDRESSES,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

def add_gcp_connection(**kwargs):
    new_conn = Connection(
            conn_id="google_cloud_default",
            conn_type='google_cloud_platform',
    )
    extra_field = {
        "extra__google_cloud_platform__scope": "https://www.googleapis.com/auth/cloud-platform",
        "extra__google_cloud_platform__project": "job-listings-366015",
        "extra__google_cloud_platform__key_path": os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    }

    session = settings.Session()

    #checking if connection exist
    if session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).first():
        my_connection = session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).one()
        my_connection.set_extra(json.dumps(extra_field))
        session.add(my_connection)
        session.commit()
    else: #if it doesn't exit create one
        new_conn.set_extra(json.dumps(extra_field))
        session.add(new_conn)
        session.commit()

dag = DAG(
    "gcp_connection", 
    default_args=default_args, 
    schedule_interval="@once",
    tags=['initial-config'],
)

with dag:
    activateGCP = PythonOperator(
        task_id='add_gcp_connection',
        python_callable=add_gcp_connection,
        provide_context=True,
    )

    activateGCP