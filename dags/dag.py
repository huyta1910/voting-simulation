# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# import sys
# sys.path.append('/opt/airflow/src')
# from data.fetch_data import generate_voter_data
# from data.db_utils import insert_voters
# import psycopg2

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# def fetch_voter_task():
#     # Fetch one voter and ingest to MinIO
#     voter = generate_voter_data(ingest_to_minio=True)
#     return voter

# def ingest_postgres_task(ti):
#     # Pull voter from XCom
#     voter = ti.xcom_pull(task_ids='fetch_voter')
#     conn = psycopg2.connect("host=postgres dbname=voting user=postgres password=postgres")
#     cur = conn.cursor()
#     insert_voters(conn, cur, voter)
#     conn.close()

# dag = DAG(
#     'voting_workflow',
#     default_args=default_args,
#     description='Orchestrate fetch and ingest of one voter record',
#     schedule_interval=timedelta(days=1),
#     start_date=datetime(2025, 8, 8),
#     catchup=False,
# )

# fetch_voter = PythonOperator(
#     task_id='fetch_voter',
#     python_callable=fetch_voter_task,
#     dag=dag,
# )

# ingest_postgres = PythonOperator(
#     task_id='ingest_postgres',
#     python_callable=ingest_postgres_task,
#     dag=dag,
# )

# fetch_voter >> ingest_postgres
