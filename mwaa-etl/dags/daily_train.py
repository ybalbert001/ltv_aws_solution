from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
from datetime import timedelta, datetime
import re
import os
import pendulum
from model_sensor import check_both_model

DAG_ID = os.path.basename(__file__).replace(".py", "")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2013, 7, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

sql_bucket = Variable.get("s3_sql_bucket")
role_arn = Variable.get("role_arn")

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
def get_sql_content(key, bucket_name, role_arn=""):
    sqls = get_object(key, bucket_name)
    _sql = remove_comments(sqls)
    sql_list = _sql.replace("\n", "").split(";")
    sql_list_trim = [ sql.format(bucket_name=bucket_name, iam_role_arn=role_arn).strip() for sql in sql_list if sql.strip() != ""]
    return list(map(lambda x: x + ";", sql_list_trim))

with DAG(
        dag_id=DAG_ID,
        description="redshift sql etl",
        default_args=default_args,
        start_date=datetime(2013, 7, 1),
        dagrun_timeout=timedelta(hours=24),
        catchup=False,
        schedule_interval='0 2 * * *',
        tags=['redshift_sql'],
) as dag:
    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    # create ods table
    task_create_table = RedshiftSQLOperator(
        redshift_conn_id="redshift_default",
        task_id='create_table',
        sql=get_sql_content('sqls/create_table.sql', sql_bucket)
    )

    task_combine_data = RedshiftSQLOperator(
        redshift_conn_id="redshift_default",
        task_id='combine_data',
        sql=get_sql_content('sqls/daily_combine_mv2table.sql', sql_bucket)
    )

    task_gen_feature1 = RedshiftSQLOperator(
        redshift_conn_id="redshift_default",
        task_id='gen_feature1',
        sql=get_sql_content('sqls/daily_update_feature_part1.sql', sql_bucket)
    )

    task_gen_feature2 = RedshiftSQLOperator(
        redshift_conn_id="redshift_default",
        task_id='gen_feature2',
        sql=get_sql_content('sqls/daily_update_feature_part2.sql', sql_bucket)
    )

    task_gen_feature3 = RedshiftSQLOperator(
        redshift_conn_id="redshift_default",
        task_id='gen_feature3',
        sql=get_sql_content('sqls/daily_update_feature_part3.sql', sql_bucket)
    )

    task_gen_label = RedshiftSQLOperator(
        redshift_conn_id="redshift_default",
        task_id='gen_label',
        sql=get_sql_content('sqls/daily_update_label.sql', sql_bucket)
    )

    task_gen_dataset = RedshiftSQLOperator(
        redshift_conn_id="redshift_default",
        task_id='gen_dataset',
        sql=get_sql_content('sqls/daily_update_dataset.sql', sql_bucket)
    )

    task_gen_classification_model = RedshiftSQLOperator(
        redshift_conn_id="redshift_default",
        task_id='gen_regression_model_v2',
        sql=get_sql_content('sqls/daily_update_regression_model_v2.sql', sql_bucket, role_arn)
    )

    task_gen_regression_model = RedshiftSQLOperator(
        redshift_conn_id="redshift_default",
        task_id='gen_regression_model_v1',
        sql=get_sql_content('sqls/daily_update_regression_model_v1.sql', sql_bucket, role_arn)
    )

    # wait_for_while = BashOperator(
    #     task_id="wait_5400s",
    #     bash_command="sleep 5400",
    # )

    wait_for_model = PythonSensor(
        task_id='wait_model_training',
        poke_interval=120,
        timeout=24*60*60,
        python_callable=check_both_model,
        op_kwargs={'dt' : '{{ ds_nodash }}'}
    )

    task_eval_model = RedshiftSQLOperator(
        redshift_conn_id="redshift_default",
        task_id='eval_model',
        sql=get_sql_content('sqls/daily_update_model_eval.sql', sql_bucket)
    )

    begin >> task_create_table >> task_combine_data >> [task_gen_feature1, task_gen_feature2, task_gen_feature3, task_gen_label] >> task_gen_dataset >> [task_gen_classification_model, task_gen_regression_model] >> wait_for_model >> task_eval_model >> end
