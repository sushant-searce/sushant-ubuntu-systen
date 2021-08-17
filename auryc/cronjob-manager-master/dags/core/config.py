import os

from datetime import timedelta
from .utils import Utils


class Config(object):
    def __init__(self):
        self.current_profile = os.getenv('APP_PROFILE', "dev")
        self.kafka_host = "kafka-1.auryc.{self.current_profile}:9092,kafka-2.auryc.{self.current_profile}:9092,kafka-3.auryc.{self.current_profile}:9092"
        self.kafka_topic = "session-replay-movie-maker"
        self.s3_host = f"http://s3.auryc.{self.current_profile}:7480"
        self.rabbitmq_host = f"rabbitmq.auryc.{self.current_profile}"
        self.rabbitmq_port = 5672
        self.rabbitmq_user = "bender"
        self.rabbitmq_password = "Auryc123!"
        self.rabbitmq_vhost = "/session-replay-movie-maker"
        self.elasticsearch_address = f'elasticsearch-1.auryc.{self.current_profile},elasticsearch-2.auryc.{self.current_profile},elasticsearch-3.auryc.{self.current_profile}'
        # Init different profiles
        if "prod" == self.current_profile:
            self.db_host = "rds.auryc.com"
            self.db_user = "root"
            self.db_password = "aU$yCP&0D"
            self.db_name = "session_replay"
            self.aws_access_key_id = "NBI29HR9ELD1QIYNVFEU"
            self.aws_secret_access_key = "SnmaCNkNkYKCNeRsBlkyIK2YyRLByzPOzCLg1Pl6"
            self.elk_address = f'malthael-1.auryc.{self.current_profile},malthael-2.auryc.{self.current_profile},malthael-3.auryc.{self.current_profile}'
        elif "cn" == self.current_profile:
            self.db_host = "rds.svc.auryc.cn"
            self.db_user = "auryc"
            self.db_password = "kQpo7s!amHp_*dcdB"
            self.db_name = "session_replay"
            self.aws_access_key_id = "AKIAQGP3V3UXZU2NEQ7T"
            self.aws_secret_access_key = "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"
            self.elk_address = f'malthael.svc.auryc.{self.current_profile}'
            self.elasticsearch_address = f'elasticsearch-1.svc.auryc.{self.current_profile},elasticsearch-2.svc.auryc.{self.current_profile},elasticsearch-3.svc.auryc.{self.current_profile}'
            self.s3_host = f"s3.cn-northwest-1.amazonaws.com.cn"
            self.kafka_host = "kafka-1.svc.auryc.{self.current_profile}:9092,kafka-2.svc.auryc.{self.current_profile}:9092,kafka-3.svc.auryc.{self.current_profile}:9092"
            self.rabbitmq_host = f"rabbitmq.svc.auryc.{self.current_profile}"
        else:
            self.db_host = "feedbackdb-dev.auryc.com"
            self.db_user = "root"
            self.db_password = "admin12#$"
            self.db_name = "session_replay"
            self.aws_access_key_id = "FFQ0Z5G2BIC02DDEQYX3"
            self.aws_secret_access_key = "yiVmLgzm6b0q4lsPCBCW0TDFfj6Gkp3JrY1WLelU"
            self.elk_address = f'malthael.auryc.{self.current_profile}'
        self.airflow_args = {
            'owner': 'Auryc Inc.',
            'depends_on_past': False,
            'start_date': Utils.start_date(),
            'email': ['ning.zhang@auryc.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        }

    def is_production_mode(self):
        if self.current_profile == 'prod':
            return True
        else:
            return False
