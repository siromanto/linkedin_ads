import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from LinkedinAds.etl import extract, load, transform

default_args = {
    'owner': 'afuser',
    'depends_on_past': False,
    'email': ['data-hub-service@condati.pagerduty.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
    'start_date': datetime(2019, 4, 2),
    'priority_weight': 20
}


def initialize():
    pass


# with open('credentials/LinkedinAdsKeys/Linkedin1.json') as f:
#     client_conf = json.load(f)

dag_name = 'LinkedinAds.v0'
dag = DAG(
    dag_name,
    default_args=default_args,
    # max_active_runs=1,
    concurrency=3,
    schedule_interval='0 6 * * *')

extract_op = PythonOperator(task_id='extract_LinkedinAds',
                            dag=dag,
                            python_callable=extract.extract_daily,
                            trigger_rule='all_success',
                            provide_context=True,
                            # op_args=[client_name],
                            execution_timeout=timedelta(minutes=140))

load_op = PythonOperator(task_id='load_LinkedinAds',
                         dag=dag,
                         python_callable=load.load_daily,
                         trigger_rule='all_success',
                         provide_context=True,
                         # op_args=[client_name],
                         execution_timeout=timedelta(minutes=15))

transform_op = PythonOperator(task_id='transform_LinkedinAds',
                              dag=dag,
                              python_callable=transform.transform_daily,
                              trigger_rule='all_success',
                              provide_context=True,
                              # op_args=[client_name],
                              execution_timeout=timedelta(minutes=15))

extract_op >> load_op >> transform_op

globals()[dag_name] = dag

dag = None  # remove temporary dag from globals


def finalize():
    pass
