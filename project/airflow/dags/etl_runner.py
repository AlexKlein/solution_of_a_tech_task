"""
A runner of the ETL process.
"""
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

sys.path.insert(0, "/usr/local/airflow/app")

from run import start_app


default_args = {
    'owner': 'AlexKlein',
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 6),
    'email': ['no-reply@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG('etl_runner',
         description='A runner for ETL tool',
         start_date=datetime(2021, 5, 7),
         schedule_interval=timedelta(days=1),
         max_active_runs=1,
         catchup=False) as dag:

    t0 = DummyOperator(task_id='dummy_task',
                       retries=3)
    t1 = PythonOperator(task_id='python_task',
                        python_callable=start_app,
                        dag=dag)
    t1.doc_md = """\
        #### Task Documentation
        This is a runner for ETL processes.
        """

    t0 >> t1
