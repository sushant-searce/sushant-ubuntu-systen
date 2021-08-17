from elasticsearch import Elasticsearch
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from core import Utils

from core import Config


config = Config()

dag = DAG('clean_up_huge_autocomplete_indices_executor', catchup=False, default_args=config.airflow_args, schedule_interval='*/20 * * * *')


class RetentionExecutor(object):
    INDEX_SIZE_DELETE_THRESHOLD = 512
    INACTIVE_SITE_IDS = ["120-httpspaysacom", "122-youworldus", "simplecitizen-com-96", "235-hackerrankcomcommunity", "avocode-com-102", 
            "207-hackerrankcom", "664-vestuacom", "693-revitalcouk", "611-embodiedphilosophycom", "625-serenovacom", "643-plumvoicecom",
            "612-avalonkingcom"]
    MONITOR_INDEX_NAME = 'c01e1dabd85b6014c53dd5ee5ad49e79'

    def __init__(self):
        self.client = None
        self.initClient()

    def initClient(self):
        if self.client is None:
            elasticsearch_addresses = config.elasticsearch_address
            self.client = Elasticsearch(elasticsearch_addresses.strip().split(','), timeout=60)

    def run(self):
        # Two steps to reduce the size of the indices
        # First, delete data which is older than 30 days
        # self.delete_expired_data()
        # Then, delete all indices whose size is greater than our threshold value.
        resp = self.client.indices.stats(index='_all')
        indices = resp['indices']
        for key, value in indices.items():
            index_name = key
            index_size_in_bytes = value['primaries']['store']['size_in_bytes']
            if index_size_in_bytes > RetentionExecutor.INDEX_SIZE_DELETE_THRESHOLD*1024*1024:
                print('Found huge index: {}, {}'.format(index_name, value))
                try:
                    search_result = self.client.search(index=index_name, _source=True,
                                                       sort=["creationTime:desc"], size=1,
                                                       allow_no_indices=False)
                    if search_result is None or search_result['hits']['hits'] is None or search_result['hits']['hits'][0] is None:
                        print("Can't find data of the index {}.".format(index_name))
                        continue
                except Exception as _:
                    print(search_result)
                    continue
                index_source_data = search_result['hits']['hits'][0]['_source']
                if 'clientId' not in index_source_data:
                    print("Couldn't find clientId property in the {}.".format(index_name))
                    self.client.indices.delete(index=index_name)
                    continue
                client_id = index_source_data['clientId']
                property_name = index_source_data['propertyName']
                # Delete the bigest data
                self.client.indices.delete(index=index_name)
                print("Delete the monolithic index data:{},{},{} bytes, done.".format(client_id, property_name, index_size_in_bytes))
            else:
                continue

    def delete_inactive_client_data(self):
        resp = self.client.indices.stats(index='_all')
        indices = resp['indices']
        for key, value in indices.items():
            index_name = key
            try:
                search_result = self.client.search(index=index_name, _source=True,
                        sort=["creationTime:desc"], size=1,
                        allow_no_indices=False)
                if search_result is None or search_result['hits']['hits'] is None or search_result['hits']['hits'][0] is None:
                    print("Can't find data of the index {}.".format(index_name))
                    continue
            except Exception as _:
                continue
            index_source_data = search_result['hits']['hits'][0]['_source']
            if 'clientId' not in index_source_data:
                print("Couldn't find clientId property in the {}.".format(index_name))
                self.client.indices.delete(index=index_name)
                continue
            client_id = index_source_data['clientId']
            if client_id in RetentionExecutor.INACTIVE_SITE_IDS:
                self.client.indices.delete(index=index_name)
                print("Remove inactive index {}.".format(index_name))


def main():
    retention_executor = RetentionExecutor()
    retention_executor.run()
    retention_executor.delete_inactive_client_data()


if __name__ == "__main__":
    main()


def check_and_filter_retention_data(ds, **kwargs):
    main()


execute_retention_job = PythonOperator(
    task_id='clean_up_huge_autocomplete_indices_job',
    provide_context=True,
    python_callable=check_and_filter_retention_data,
    dag=dag)


execute_retention_job
