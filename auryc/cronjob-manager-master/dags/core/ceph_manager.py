import logging
import boto3
from .config import Config


class CephManager(object):

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.config = Config()
        self.client = None

    def __connect(self):
        if self.client is None:
            self.client = boto3.client('s3', endpoint_url=self.config.s3_host, aws_access_key_id=self.config.aws_access_key_id, aws_secret_access_key=self.config.aws_secret_access_key)
        return self.client

    def delete(self, bucket, prefix):
        client = self.__connect()
        paginator = client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        delete_us = dict(Objects=[])
        if pages is None:
            self.logger.info("Can't find the ceph object int the bucket {} with the prefix {}".format(bucket, prefix))
            return
        for item in pages.search('Contents'):
            if item is not None:
                delete_us['Objects'].append(dict(Key=item['Key']))
            if len(delete_us['Objects']) >= 1000:
                client.delete_objects(Bucket=bucket, Delete=delete_us)
                delete_us = dict(Objects=[])
        if len(delete_us['Objects']):
            client.delete_objects(Bucket=bucket, Delete=delete_us)
