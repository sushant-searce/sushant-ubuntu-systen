import hashlib
import uuid

from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from core import Config, RdsManager, CephManager, Utils

MAIN_DAG_NAME = 'session-replay-data-cleaner'
SCHEDULE_INTERVAL = '*/5 * * * *'
TOTAL_DELETE_COUNT = 10000
BATCH_DELETE_COUNT = 2500

config = Config()
main_dag = DAG(MAIN_DAG_NAME, catchup=False, default_args=config.airflow_args, schedule_interval=SCHEDULE_INTERVAL)


class SessionReplayData(object):

    def __init__(self, site_id, session_id):
        self.site_id = site_id
        self.session_id = session_id

    def __str__(self):
        return "{}".format(self.__dict__)


class SessionReplayDataCleaner(object):

    def __init__(self):
        self.db_manager = RdsManager()
        self.ceph_manager = CephManager()
        self.bucket = "session-replay-recording-data"

    def countExpiredData(self):
        min_wait_time = Utils.now().subtract(hours=2).int_timestamp*1000
        sql = "SELECT count(id) as total_count FROM tracking_sessions WHERE (last_process_time <={} AND status = 1)".format(min_wait_time)
        row = self.db_manager.get(sql)
        return row['total_count']

    def fetchExpiredData(self, limit):
        min_wait_time = Utils.now().subtract(hours=2).int_timestamp*1000
        sql = "SELECT id, site_id FROM tracking_sessions WHERE (last_process_time <={} AND status = 1) limit {}".format(min_wait_time, limit)
        rows = self.db_manager.find(sql)
        result = list(map(lambda row: SessionReplayData(row['site_id'], row['id']), rows))
        return result

    def deleteExpiredSingleData(self, session_replay_data):
        m = hashlib.md5()
        m.update(session_replay_data.site_id.encode('utf-8'))
        m.update(session_replay_data.session_id.encode('utf-8'))
        prefix = m.hexdigest()
        try:
            sql = "DELETE FROM tracking_sessions WHERE id='{}' AND status = 1".format(session_replay_data.session_id)
            self.db_manager.execute(sql)
            self.ceph_manager.delete(self.bucket, prefix)
            print("Delete expired data {} from database and ceph with the prefix {} done.".format(session_replay_data, prefix))
            return True
        except Exception as e:
            print("Failed to delete {} caused by {}".format(session_replay_data, str(e)))
            return False

    def deleteExpiredData(self, expired_sessions):
        count = 0
        if expired_sessions is not None:
            for session in expired_sessions:
                is_done = self.deleteExpiredSingleData(session)
                if is_done:
                    count += 1
        return count

    def run(self):
        print(self.countExpiredData())
        expired_sessions = self.fetchExpiredData(10)
        if expired_sessions is None:
            print("Empty data.")
            return
        count = self.deleteExpiredData(expired_sessions)
        print("Has deleted {} expired sessions of the total {} records from storage.".format(count, len(expired_sessions)))


cleaner = SessionReplayDataCleaner()


if __name__ == "__main__":
    cleaner.run()

def countChunkCount():
    tasks = int(TOTAL_DELETE_COUNT/BATCH_DELETE_COUNT)
    if TOTAL_DELETE_COUNT % BATCH_DELETE_COUNT != 0:
        tasks += 1
    return tasks


total_chunk_count = countChunkCount()

def start_job(**context):
    total_count = cleaner.countExpiredData()
    print("We have found {} expired sessions.".format(total_count))
    return total_count


start = PythonOperator(
    task_id='start',
    provide_context=True,
    python_callable=start_job,
    dag=main_dag)

def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

def fetch_and_dispatch_expired_sessions_job(**context):
    expired_sessions = cleaner.fetchExpiredData(TOTAL_DELETE_COUNT)
    print("We've found {} expired sessions to delete at {}.".format(len(expired_sessions), str(uuid.uuid4())))

    if len(expired_sessions):
        index = 0
        for arr in chunks(expired_sessions, BATCH_DELETE_COUNT):
            index += 1
            print("The task cleaner-{} has assigned {} sessions.".format(str(index), len(arr)))
            context['ti'].xcom_push(key="cleaner-{}-assigned-chunk".format(index), value=arr)


fetch_and_dispatch_expired_sessions = PythonOperator(
    task_id='fetch_and_dispatch_expired_sessions',
    provide_context=True,
    python_callable=fetch_and_dispatch_expired_sessions_job,
    dag=main_dag)


def delete_expired_sessions_job(**context):
    current_id = context["params"]["cleaner_id"]
    expired_sessions = context["ti"].xcom_pull(key="cleaner-{}-assigned-chunk".format(current_id), task_ids='fetch_and_dispatch_expired_sessions')
    count = 0
    if expired_sessions is not None and len(expired_sessions):
        count = cleaner.deleteExpiredData(expired_sessions)
        print("Current cleaner task {} have fetched {} expired sessions to delete.".format(current_id, len(expired_sessions)))
    return count

def end_job(**context):
    count = 0
    for i in range(total_chunk_count):
        finished_count = context['ti'].xcom_pull(key=None, task_ids='cleaner-{}'.format(str(i+1)))
        if finished_count is None:
            finished_count = 0
        count += finished_count
    print("{}.The End.".format(count))


end = PythonOperator(
    task_id='end',
    provide_context=True,
    python_callable=end_job,
    trigger_rule='all_done',
    dag=main_dag)

start.set_downstream(fetch_and_dispatch_expired_sessions)

for index in range(total_chunk_count):
    task = PythonOperator(
        task_id='cleaner-{}'.format(str(index+1)),
        provide_context=True,
        python_callable=delete_expired_sessions_job,
        params={"cleaner_id": str(index+1)},
        dag=main_dag)
    task.set_upstream(fetch_and_dispatch_expired_sessions)
    task.set_downstream(end)
