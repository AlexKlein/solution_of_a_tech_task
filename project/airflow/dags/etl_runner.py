"""
A runner of the ETL process from MySQL to PostgreSQL.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import BashOperator


default_args = {
    'owner': 'AlexKlein',
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 5),
    'email': ['no-reply@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_runner',
    default_args=default_args,
    description='A runner for ETL tool',
    schedule_interval=timedelta(days=1),
)

t1 = BashOperator(
    task_id='start_etl',
    bash_command='python /usr/local/airflow/app/run.py',
    dag=dag)

dag.doc_md = __doc__

t1.doc_md = """\
#### Task Documentation
This is a runner for ETL processes.
"""
