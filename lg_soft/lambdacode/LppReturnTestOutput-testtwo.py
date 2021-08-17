import time
import json
import boto3
import os
import time
from urllib.request import urlopen
import ast

def handler(event, context):

    orchestration_engine="Airflow"
    airflow_endpoint="ec2-3-36-41-229.ap-northeast-2.compute.amazonaws.com"
    dynamodbclient = boto3.resource('dynamodb')
    table_name = 'LppMLOpList-3l7ajky2jnhujexm7b7tsmlt6u-dev'
    table = dynamodbclient.Table(table_name)
    wf_exec_id = event['execution_id']
    exec_arn = "LG-dynamic-"+str(wf_exec_id)
    dag_idd = exec_arn
    response = None
    
    while response is None or 'running':
        time.sleep(2)
        try:
            airflow_status_url = 'http://{}:8080/api/experimental/dags/{}/dag_runs'.format(airflow_endpoint,dag_idd)
            dags_status = urlopen(airflow_status_url).read()
            print(dags_status)
            status = ast.literal_eval(dags_status.decode('utf-8'))
            response = status[0]['state']
        except:
            pass
        
        if response == 'success':
            s3_location = event['output_loc']
            bucket_name = s3_location.split('/')[2]
            output_key = '/'.join(s3_location.split('/')[3:])
            if s3_location[-3:] == 'txt':
                s3resource = boto3.resource('s3')
                object = s3resource.Object(bucket_name, output_key)
                body = object.get()['Body'].read().decode('utf-8')
                print(body)
            else:
                body = 'Test has completed successfully. Kindly download the output file to see the result.'
            s3client = boto3.client('s3')
            output_url = s3client.generate_presigned_url('get_object', 
                                                        Params={'Bucket': bucket_name, 'Key': output_key, 'ResponseContentDisposition': 'attachment'},
                                                        ExpiresIn=300)
            break
        elif response == 'running':
            print("i am at running")
            continue
        elif response == 'failed':
            body = "Test has failed"
            output_url = ""
            break
        else:
            print("Dag has not yet started")
            continue
            
    return {
        'body':body,
        'output_url': output_url
    }