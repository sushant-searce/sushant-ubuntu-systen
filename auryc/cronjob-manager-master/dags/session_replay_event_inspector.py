import time
import json
import logging
from pprint import pprint
from datetime import datetime, timedelta

import pymysql.cursors
import pika
import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from airflow.models import Variable

from core import Config, Utils, RdsManager

config = Config()
dag = DAG('session_replay_event_inspector', catchup=False, default_args=config.airflow_args, schedule_interval='1-59/2 * * * *')


class MessageDeliverer(object):

    def __init__(self):
        self.virtual_host = "/session-replay-movie-maker"
        self.exchange = "pre-movie-maker"
        self.queue = "pre-movie-maker"
        self.routing_key = "pre-movie-maker"

        credentials = pika.PlainCredentials(config.rabbitmq_user, config.rabbitmq_password)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=config.rabbitmq_host,
                                                                            port=config.rabbitmq_port,
                                                                            virtual_host=self.virtual_host,
                                                                            credentials=credentials))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange,
                                      exchange_type='direct',
                                      durable=True,
                                      auto_delete=False,
                                      passive=False)
        self.channel.confirm_delivery()
        self.channel.add_on_return_callback(callback=lambda channel, method, properties, body: print(f"Message {body} was returned."))

    def send(self, msg):
        try:
            self.channel.basic_publish(exchange=self.exchange,
                                          routing_key=self.routing_key,
                                          body=msg,
                                          properties=pika.BasicProperties(content_type='application/json', delivery_mode=1),
                                          mandatory=True)
            return True
        except Exception as ex:
            print(str(ex))
            return False

    def close(self):
        self.connection.close()


class TrackingSessionMessage(object):

    def __init__(self, site_id, session_id, status):
        self.siteId = site_id
        self.sessionId = session_id
        self.status = status

    def to_json(self):
        return json.dumps(self.__dict__)

    def __str__(self):
        return "Site ID:%s, Session ID:%s, Status:%s " % (self.siteId, self.sessionId, self.status)


class EventInspector(object):

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.rds_manager = RdsManager()

    def run(self):
        message_deliver = MessageDeliverer()
        connection = self.rds_manager.connect()
        if not connection or not message_deliver:
            self.logger.warning('Failed to init required resources.')
            return
        try:
            current_milli_time = Utils.current_timestamp()
            sql = f"SELECT * FROM `tracking_sessions` WHERE ((({current_milli_time}- `last_visit_time`) >  1800000 AND (status =0 or status =4) ) OR (last_visit_time > last_process_time AND (status =2 OR status =4))) ORDER BY last_visit_time DESC LIMIT 6000"
            print(sql)
            with connection.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
                if not rows:
                    print('No suitable data found.')
                    return
                for row in rows:
                    print(f"Found row: {str(row)}, convert it to a message.")
                    status = 4
                    last_visit_time = row['last_visit_time']
                    if(current_milli_time - last_visit_time >= 1800000):
                        status = 1
                    message = TrackingSessionMessage(row['site_id'], row['id'], status)
                    rel = message_deliver.send(message.to_json())
                    if rel:
                        sql = "UPDATE `tracking_sessions` SET status =3 WHERE id= %s"
                        cursor.execute(sql, (message.sessionId),)
                        connection.commit()
                        print(f"Send message: {str(message)} done.")
                    else:
                        print(f"Send message: {str(message)} failed, caused by {rel}")
        except Exception as ex:
            logging.warning(f"Failed to check and deliver message, caused by {str(ex)}")
            raise RuntimeError(str(ex))
        finally:
            connection.close()
            message_deliver.close()


def main():
    event_inspector = EventInspector()
    event_inspector.run()


if __name__ == "__main__":
    main()

session_replay_event_inspector = PythonOperator(
    task_id='inspect_session_replay_event',
    python_callable=main,
    dag=dag)

session_replay_event_inspector
