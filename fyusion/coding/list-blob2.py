from __future__ import absolute_import
import sys
from google.cloud import storage
from celery import Celery
from test_celery.celery import app
import time

def list_blobs_with_prefix(**kwargs):

    list=[]
    storage_client = storage.Client()
    bucket_name="sushant-fyusiob-dataset"
    prefix="IBUG"

    blobs = storage_client.list_blobs(
        bucket_name, prefix=prefix
    )

    print("Blobs:")
    for blob in blobs:
        # print(blob.name)
        list.append(blob.name)
    return list
