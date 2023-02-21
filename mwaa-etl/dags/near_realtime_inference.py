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

# get object from s3
def get_object(key, bucket_name):
    hook = S3Hook()
    content_object = hook.read_key(key=key, bucket_name=bucket_name)
    return content_object


# remove sql comments
def remove_comments(sqls):
    out = re.sub(r'/\*.*?\*/', '', sqls, re.S)
    out = re.sub(r'--.*', '', out)
    return out


# s3 content to sqls list
def get_sql_content(key, bucket_name):
    sqls = get_object(key, bucket_name)
    _sql = remove_comments(sqls)
    sql_list = _sql.replace("\n", "").split(";")
    sql_list_trim = [sql.strip() for sql in sql_list if sql.strip() != ""]
    return list(map(lambda x: x + ";", sql_list_trim))

DAG_ID = os.path.basename(__file__).replace(".py", "")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.strptime("2013-07-02 00:00:00",'%Y-%m-%d %H:%M:%S'),
    'end_date' : datetime.strptime("2013-07-03 00:00:00",'%Y-%m-%d %H:%M:%S'),
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
        catchup=True,
        max_active_runs=1,
        schedule_interval='*/2 * * * *',
        tags=['redshift_sql'],
) as dag:
    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    # create ods table
    task_update_real_feature = RedshiftSQLOperator(
        redshift_conn_id="redshift_default",
        task_id='update_real_feature',
        sql=get_sql_content('wmaa/sqls/near_realtime_feature.sql', 'ltv-poc')
    )

    task_model_inference = RedshiftSQLOperator(
        redshift_conn_id="redshift_default",
        task_id='model_inference',
        sql=get_sql_content('wmaa/sqls/near_realtime_infer.sql', 'ltv-poc')
    )

    begin >> task_update_real_feature >> task_model_inference >> end
