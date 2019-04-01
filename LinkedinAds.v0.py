import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from BingSearchConsole.etl import extract, load, transform

default_args = {
    'owner': 'afuser',
    'depends_on_past': False,
    'email': ['data-hub-service@condati.pagerduty.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'start_date': datetime(2019, 3, 18),
    'priority_weight': 20
}


def initialize():
    pass


with open('credentials/BingSearchConsoleKeys/BingConsole.json') as f:
    client_conf = json.load(f)

dag_name = 'BingSearchConsole.v0'
dag = DAG(
    dag_name,
    default_args=default_args,
    # max_active_runs=1,
    concurrency=3,
    schedule_interval='0 0 * * THU')

extract_op = PythonOperator(task_id='extract_BingSearchConsole',
                            dag=dag,
                            python_callable=extract.extract_weekly,
                            trigger_rule='all_success',
                            provide_context=True,
                            # op_args=[client_name],
                            execution_timeout=timedelta(minutes=140))

load_op = PythonOperator(task_id='load_BingSearchConsole',
                         dag=dag,
                         python_callable=load.load_weekly,
                         trigger_rule='all_success',
                         provide_context=True,
                         # op_args=[client_name],
                         execution_timeout=timedelta(minutes=15))

transform_op = PythonOperator(task_id='transform_BingSearchConsole',
                              dag=dag,
                              python_callable=transform.transform_weekly,
                              trigger_rule='all_success',
                              provide_context=True,
                              # op_args=[client_name],
                              execution_timeout=timedelta(minutes=15))

extract_op >> load_op >> transform_op

globals()[dag_name] = dag

dag = None  # remove temporary dag from globals


def finalize():
    pass
