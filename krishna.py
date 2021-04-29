import sys
from datetime import datetime, timedelta
from airflow import DAG sys
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

sys.path.append('/Users/gv/airflow/dags/examples/return_analytics')

from damage_return_etl import damageReturnETL

default_args = {
'owner': 'gri',
'depends_on_past': False,
'start_date': datetime(2020, 12, 4),
'email': ['gri@everforce.com'],
'email_on_failure': True,
'email_on_retry': True,
'retries': 1,
'retry_delay': timedelta(minutes=5),
,
}

dag = DAG('DAG_damage_return_etl', default_args=default_args, schedule_interval="*/30 * * * *")

process_dag = PythonOperator(
task_id='task_id1',
python_callable=damageReturnETL,
dag=dag)