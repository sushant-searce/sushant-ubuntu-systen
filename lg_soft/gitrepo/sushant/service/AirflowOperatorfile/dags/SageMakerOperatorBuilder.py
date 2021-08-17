import yaml
import json
import boto3
import uuid
import base64
import time
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook

class SageMakerOperatorBuilder:
    
    def callableFunction(self,taskid,states,context,**kwargs):
        s3client = boto3.client('s3')
        region = s3client.meta.region_name
        print('region is',region)
        
        taskname = taskid.replace('_',' ')
        parameters = states.get(taskname).get('Parameters')
        print(parameters)
        transformjobname = 'BatchInference-'+str(uuid.uuid1())

        client = boto3.client('sagemaker',region_name=region)
        print(parameters) 
        response_model = client.create_model(
                ModelName=parameters['ModelName'],
                PrimaryContainer={
                    'Image': "121021644041.dkr.ecr.ap-southeast-1.amazonaws.com/sagemaker-xgboost:1.2-1",
                    'ImageConfig': {
                    'RepositoryAccessMode': 'Platform'
                    },
                    'Mode': 'SingleModel',
                    'ModelDataUrl': 's3://cdk-modules-s3workflowbaselocationb2150c8a-hr04a6owehy3/model.tar.gz',
                },
                ExecutionRoleArn='arn:aws:iam::797237262327:role/sagemaker-modules-sagemakerexecutionrole0D136CE5-120GAFR3HTKCW',
                Tags=[{'Key':'user','Value':context['user']},
                    {'Key':'team','Value':context['team']},
                    {'Key':'department','Value':context['department']},
                    {'Key':'executionId','Value':context['executionId']}],    
                EnableNetworkIsolation=False
            )
    
        time.sleep(5)

        response = client.create_transform_job(
            TransformJobName= transformjobname,
            ModelName=parameters.get('ModelName'),
            BatchStrategy=parameters.get('BatchStrategy'),
            Environment={
                'string': 'string'
            },
            TransformInput={
                'DataSource': {
                    'S3DataSource': {
                        'S3DataType': parameters.get('TransformInput').get('DataSource').get('S3DataSource').get('S3DataType'),
                        'S3Uri': parameters.get('TransformInput').get('DataSource').get('S3DataSource').get('S3Uri')
                        }
                },
                'ContentType': 'text/csv',
                'CompressionType': 'None',
                'SplitType': 'Line'
            },
            TransformOutput={
                'S3OutputPath': parameters.get('TransformOutput').get('S3OutputPath'),
                'Accept': 'text/csv',
                'AssembleWith': 'Line'
            },
            TransformResources={
                'InstanceType': parameters.get('TransformResources').get('InstanceType'),
                'InstanceCount': parameters.get('TransformResources').get('InstanceCount')
            },
            DataProcessing={
                'InputFilter': '$[1:]',
                'OutputFilter': '$',
                'JoinSource': 'None'
            }
        )
        print(response)
        time.sleep(5)
        while True:
            time.sleep(2)
            transform_job_response = client.describe_transform_job(
                        TransformJobName=transformjobname,
                        )
            
            status = transform_job_response.get('TransformJobStatus')
            if status =='Completed':
                break
            elif status == 'Failed':
               raise ValueError('Batch Job Transform is fail on Sagemaker')
        print("Execution is Complete")

    def getOperator(self, dag, task_id, states, context):
        operator = PythonOperator(
	    task_id = task_id,
	    provide_context=True,
	    python_callable = SageMakerOperatorBuilder().callableFunction,
            op_kwargs={'taskid': task_id, 'states': states, 'context': context},
	    dag = dag,
	)
        return operator
