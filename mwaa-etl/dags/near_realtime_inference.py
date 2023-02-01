from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from datetime import timedelta, datetime
import re
import os

DAG_ID = os.path.basename(__file__).replace(".py", "")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.strptime("2013-07-02 00:00:00",'%Y-%m-%d %H:%M:%S'),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
        dag_id=DAG_ID,
        description="redshift sql etl",
        default_args=default_args,
        dagrun_timeout=timedelta(hours=24),
        catchup=False,
        schedule_interval='*/5 * * * *',
        tags=['redshift_sql'],
) as dag:
    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    # create ods table
    task_update_real_feature = RedshiftSQLOperator(
        redshift_conn_id="redshift_default",
        task_id='update_real_feature',
        sql='s3://ltv-poc/wmaa/sqls/near_realtime_feature.sql'
    )

    task_model_inference = RedshiftSQLOperator(
        redshift_conn_id="redshift_default",
        task_id='model_inference',
        sql='s3://ltv-poc/wmaa/sqls/near_realtime_infer.sql'
    )

    begin >> task_update_real_feature >> task_model_inference >> end
