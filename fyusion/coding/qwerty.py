from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
     'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'testing_v1', default_args=default_args, schedule_interval=None)


start = DummyOperator(task_id='run_this_first', dag=dag)

def print_context(ds, **kwargs):
    #parameter = kwargs['dag_run'].conf["parameter"]
    parameter = kwargs['dag_run'].conf["file_variable"]
    print("received parameter: ", parameter)
    global variable
    variable = parameter
    print(variable)
    print(variable)
    print(variable)
    print(variable)
    print(variable)
    print(variable)
    return 'Whatever you return gets printed in the logs'

run_this = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag)



sql_alchemy_conn=postgresql+psycopg2://airflow:airflow@10.200.130.32/airflow


result_backend=db+postgresql://airflow:airflow@10.200.130.32/airflow
