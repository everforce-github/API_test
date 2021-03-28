from __future__ import print_function, division
import sys
import json
from datetime import datetime, timedelta,date, time

from airflow import DAG
from airflow.models import Variable
from airflow import settings
from airflow.models import Connection

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook

import snowflake.connector
import logging

#sys.path.append('/home/ubuntu/airflow/operators')
sys.path.append('/Users/gv/airflow/operators')
import globals_for_dags

def get_logger(logger_name, create_file=False):

        log = logging.getLogger(logger_name)
        log.setLevel(level=logging.INFO)

        # create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        if create_file:
            # create file handler for logger.
            fh = logging.FileHandler(globals_for_dags.LOG_FILE_NAME)
            fh.setLevel(level=logging.DEBUG)
            fh.setFormatter(formatter)
        # create console handler for logger.
        ch = logging.StreamHandler()
        ch.setLevel(level=logging.DEBUG)
        ch.setFormatter(formatter)

        # add handlers to logger.
        if create_file:
            log.addHandler(fh)

        log.addHandler(ch)
        return log


def myCreateConn(conn_id, conn_type, host, login, password, port, extra):
    conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
        login=login,
        password=password,
        port=port,
        extra=extra
    )
    mylog.info("create_conn() for " + conn_id +" "+ host +" "+ conn_type)
    session = settings.Session()
    conn_name = session\
    .query(Connection)\
    .filter(Connection.conn_id == conn.conn_id)\
    .first()

    if str(conn_name) == str(conn_id):
        return mylog.info("Connection " + conn_id + " already exists")

    session.add(conn)
    session.commit()
    mylog.info(Connection.log_info(conn))
    mylog.info("Connection" + conn_id +" is created")


mylog = get_logger(globals_for_dags.LOGGER_NAME, create_file=True)

default_args = {
    'owner': 'Gri',
    'depends_on_past': False,
    'start_date': datetime(2021,2,12,2,0),
    'end_date': datetime(2021,2,13,12,0),
    'email': ['gri@everforce.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

mydag = DAG(
    'wf_name_dag1', default_args=default_args, catchup=False, schedule_interval="*/30 * * * *")

SF_conn_task = PythonOperator(
    task_id='JT_GRI_SCRIPT_TEST_JOB_task_1',
    provide_context=True,
    python_callable=myCreateConn,
    op_kwargs={
        'conn_id': 'a_my_SF_conn',
        'conn_type': 'Sqlite',
        'host': 'fza75044.snowflakecomputing.com',
        'login' : 'Everforce Demo1',
        'password':'1everforce23',
        'port':'0',  # not used
        'extra': '{ "account": "fza75044", "warehouse": "DEMO_WH", "database": "DEMO1_DB" }'},
    dag=mydag
)

def etl_function():
    try:
        sf_hook = SnowflakeHook(snowflake_conn_id="a_my_SF_conn")
        cursor = sf_hook.get_cursor()
        cursor.execute("select * from JT_GRI_SCRIPT_TEST_JOB where run_task = 'YES' order by execution_order")
        mylog.info("Total number of rows is: " + str(cursor.rowcount))
        records = cursor.fetchall()
        for row in records:
            sql_cmd = row[4] # remove hardcoding later
            if sql_cmd.startswith('\"'): sql_cmd = sql_cmd[1:]
            if sql_cmd.endswith('\"'): sql_cmd = sql_cmd[:-1]
            if sql_cmd.startswith('//'):
                    continue
            cursor.execute(sql_cmd)
            mylog.info('SQL:' + sql_cmd)

        mylog.info("Job completed\n")

    except:
        mylog.exception("SQL:" +"["+sql_cmd+"]\n")
        mylog.error("ETL task not completed as above error occured")
    finally:
        cursor.close()
        mylog.info("Snowflake ETL task completed")


sf_sql_cmd = PythonOperator(
    task_id='JT_GRI_SCRIPT_TEST_JOB_task_2',
    dag=mydag,
    python_callable=etl_function
)

SF_conn_task >> sf_sql_cmd


from get_SF_table2 import table_row_count

wf_task_id_1= PythonOperator(
task_id='wf_task_id_1',
python_callable=table_row_count,
dag=mydag)

from sql_operator import mysql_function

wf_task_id_3= PythonOperator(
task_id='wf_task_id_3',
python_callable=mysql_function,
op_kwargs={'mysql_connection_id': 'a_mysql_100.20.213.72'},
dag=mydag)

wf_task_id_4= PythonOperator(
task_id='wf_task_id_4',
python_callable=mysql_function,
op_kwargs={'mysql_connection_id': 'a_mysql_100.20.213.72'},
dag=mydag)

wf_task_id_2= PythonOperator(
task_id='wf_task_id_2',
python_callable=mysql_function,
op_kwargs={'mysql_connection_id': 'a_mysql_100.20.213.72'},
dag=mydag)

depedent1_task_id= PythonOperator(
task_id='depedent1_task_id',
python_callable=mysql_function,
op_kwargs={'mysql_connection_id': 'a_mysql_100.20.213.72'},
dag=mydag)

depedent2_task_id= PythonOperator(
task_id='depedent2_task_id',
python_callable=mysql_function,
op_kwargs={'mysql_connection_id': 'a_mysql_100.20.213.72'},
dag=mydag)

depedent3_task_id= PythonOperator(
task_id='depedent3_task_id',
python_callable=mysql_function,
op_kwargs={'mysql_connection_id': 'a_mysql_100.20.213.72'},
dag=mydag)

[wf_task_id_4,wf_task_id_1,wf_task_id_3]  >> wf_task_id_2
wf_task_id_2 >> depedent1_task_id
depedent1_task_id >> depedent2_task_id >> depedent3_task_id
