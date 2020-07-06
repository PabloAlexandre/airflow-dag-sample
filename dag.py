from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def sleep(time):
  print(f"Sleeping {time} seconds")
  time.sleep(random_base)

def welcome():
  print('Hello!')

default_args = {
    "owner": "Pablo",
    "start_date": datetime(2020, 7, 6),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
  'sample_dag',
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

for i in range(5):
  task = PythonOperator(
    task_id='sleep_for_' + str(i),
    python_callable=sleep,
    op_kwargs={'time': float(i) / 10},
    dag=dag,
  )

  hello >> task
