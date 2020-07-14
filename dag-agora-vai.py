from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import time

import requests 
import humanize


def sleep(seconds, **kwargs):
  print(f"Sleeping {seconds} seconds")
  time.sleep(seconds)

def welcome(ds, **kwargs):
  r = requests.get("https://run.mocky.io/v3/f92cda62-ec3f-4edd-bb79-d59f6fa14dfd")
  print(humanize.intword(12345591313))
  return r.text

welcome(1)

default_args = {
    "owner": "Pablo",
    "start_date": datetime(2020, 7, 6),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
  'sample_dag_agora_vai',
  default_args=default_args,
  description='Sample Dag',
  schedule_interval=timedelta(days=1),
)

hello = PythonOperator(
    task_id='hello',
    provide_context=True,
    python_callable=welcome,
    dag=dag,
)

task = PythonOperator(
  task_id='sleep_for_1',
  python_callable=sleep,
  op_kwargs={'seconds': 1 },
  dag=dag,
)

hello >> task
