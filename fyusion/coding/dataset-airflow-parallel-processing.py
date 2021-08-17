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

image = "gcr.io/ml-training-pilot/pytorch-dataset-pix2pix:v8"

dag = DAG(
    'facades-dataset-processing', default_args=default_args, schedule_interval=None)

start = DummyOperator(task_id='run_this_first', dag=dag)

pix2pix = KubernetesPodOperator(namespace='airflow-local',
                          image=image,
                          env_vars={'DATASET_VAR': param},
                          labels={"foo": "bar"},
                          name="pix2pix-test",
                          task_id="pix2pix-task",
                          get_logs=True,
                          resources={'limit_gpu' : 1},
                          dag=dag,
                          affinity={
                            'nodeAffinity': {
                                'requiredDuringSchedulingIgnoredDuringExecution': {
                                    'nodeSelectorTerms': [{
                                        'matchExpressions': [{
                                            'key': 'cloud.google.com/gke-nodepool',
                                            'operator': 'In',
                                            'values': [
                                                'gpu-pvm-pool-01',
                                            ]
                                        }]
                                    }]
                                }
                            }
                        }
                          )