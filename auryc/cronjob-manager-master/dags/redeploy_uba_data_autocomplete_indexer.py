import time
import json
import logging
from pprint import pprint
from datetime import datetime, timedelta

import requests
import pendulum
import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from airflow.models import Variable

from core import Config, Utils

config = Config()
dag = DAG('indexer_deployer', catchup=False, default_args=config.airflow_args, schedule_interval='@hourly')


class IndexerDeployer(object):

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.name = 'uba-data-autocomplete-indexer'
        self.namespace = config.current_profile
        self.url = f'http://zeus.auryc.{config.current_profile}:5000/service'

    def deploy(self):
        payload = {}
        payload['applicant'] = "ning.zhang@auryc.com"
        payload['target'] = self.name
        payload['namespace'] = self.namespace
        payload['command'] = 'deploy'
        url = f"{self.url}/command"
        try:
            resp = requests.post(url, json=payload)
            if resp.status_code == 201:
                return True
            return False
        except Exception as ex:
            print(str(ex))
            return False

    def run(self):
        try:
            resp = self.deploy()
            if resp:
                print(f"Deploy done.")
            else:
                raise RuntimeError('Failed to deploy.')
        except Exception as ex:
            logging.warning(f"Failed to deploy, caused by {str(ex)}")
            raise RuntimeError(str(ex))


def main():
    executor = IndexerDeployer()
    executor.run()


if __name__ == "__main__":
    main()

executor = PythonOperator(
    task_id='periodly_deploy_indexer',
    python_callable=main,
    dag=dag)

executor
