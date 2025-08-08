from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def start_task():
    print("Workflow started.")

def process_data():
    print("Processing data with Spark/Kafka...")

def end_task():
    print("Workflow finished.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'voting_workflow',
    default_args=default_args,
    description='This is the voting workflow',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 8, 8),
    catchup=False,
)

start = PythonOperator(
    task_id='start',
    python_callable=start_task,
    dag=dag,
)

process = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

end = PythonOperator(
    task_id='end',
    python_callable=end_task,
    dag=dag,
)

start >> process >> end
