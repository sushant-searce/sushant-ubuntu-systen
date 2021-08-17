from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator

param = "{{dag_run.conf.get('parameter')}}"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

image = "gcr.io/ml-training-pilot/pytorch-dataset-pix2pix:v5"

dag = DAG(
    'facades-dataset-parallel', default_args=default_args, schedule_interval=None)

start = DummyOperator(task_id='run_this_first', dag=dag)

start_passing = KubernetesPodOperator(namespace='default',
                          image=image,
                          env_vars={'DATASET_VAR': param},
                          labels={"foo": "bar"},
                          name="start_passing-test",
                          task_id="start_passing-task",
                          get_logs=True,
                          resources={'limit_cpu' : 1},
                          dag=dag,
                          affinity={
                            'nodeAffinity': {
                                'requiredDuringSchedulingIgnoredDuringExecution': {
                                    'nodeSelectorTerms': [{
                                        'matchExpressions': [{
                                            'key': 'cloud.google.com/gke-nodepool',
                                            'operator': 'In',
                                            'values': [
                                                'standard-pool-1',
                                            ]
                                        }]
                                    }]
                                }
                            }
                        }
                          )