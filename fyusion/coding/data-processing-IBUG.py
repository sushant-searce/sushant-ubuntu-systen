from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
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

image = "gcr.io/ml-training-pilot/fyusion-redis-celery:t2"

dag = DAG(
    'data-processing-IBUG', default_args=default_args, schedule_interval=None)


start = DummyOperator(task_id='run_this_first', dag=dag)

IBUG = KubernetesPodOperator(namespace='airflow-local',
                          image=image,
                          labels={"foo": "bar"},
                          name="IBUG-test",
                          task_id="IBUG-task",
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