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
dag = DAG('movie_maker_size_inspector', catchup=False, default_args=config.airflow_args, schedule_interval='*/5 * * * *')


class MovieMakerSizeInspector(object):

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.name = 'session-replay-movie-maker'
        self.namespace = config.current_profile
        self.url = f'http://zeus.auryc.{config.current_profile}:5000/service'

    def get_current_size(self):
        application_id = Utils.hash(self.namespace, self.name)
        resp = requests.get(f"{self.url}/{application_id}")
        if resp.status_code != 200:
            return 0
        resp = resp.json()
        config_json_content = resp.get('config')
        if not config_json_content:
            return 0
        config = Utils.decode(config_json_content)
        config = json.loads(config)
        current_size = config.get('total_capacity', 0)
        if not current_size:
            return 0
        return int(current_size)

    def change_size(self, total_capacity):
        payload = {}
        payload['target'] = self.name
        payload['namespace'] = self.namespace
        payload['command'] = 'scale'
        payload['payload'] = {'total_capacity': total_capacity}
        url = f"{self.url}/command"
        try:
            resp = requests.post(url, json=payload)
            if resp.status_code == 201:
                return True
        except Exception as ex:
            print(str(ex))
            return False

    def is_weekend(self, day):
        if day == pendulum.SUNDAY or day == pendulum.SATURDAY:
            return True
        else:
            return False

    def get_expected_size(self):
        now = pendulum.now('UTC')
        day = now.day_of_week
        hour = now.hour
        print(f"Current day: {day}, hour:{hour}, weekend:{self.is_weekend(day)}")
        if self.is_weekend(day):
            if hour >= 8 and hour < 13:
                return 3
            else:
                return 7
        else:
            if hour >= 6 and hour < 9:
                return 7
            elif hour >= 9 and hour < 12:
                return 3
            else:
                return 12

    def run(self):
        if not config.is_production_mode():
            return
        try:
            current_size = self.get_current_size()
            expected_size = self.get_expected_size()
            if current_size != expected_size:
                resp = self.change_size(expected_size)
                if resp:
                    print(f"Change size done, from {current_size} to {expected_size}")
                else:
                    raise RuntimeError('Failed to change size.')
            else:
                print(f"Consistent size, expected size:{expected_size}, current size: {current_size}, now exit")
        except Exception as ex:
            logging.warning(f"Failed to check and update size, caused by {str(ex)}")
            raise RuntimeError(str(ex))


def main():
    inspector = MovieMakerSizeInspector()
    inspector.run()


if __name__ == "__main__":
    main()

inspector = PythonOperator(
    task_id='ensure_has_expected_size_in_specific_time_period',
    python_callable=main,
    dag=dag)

inspector
