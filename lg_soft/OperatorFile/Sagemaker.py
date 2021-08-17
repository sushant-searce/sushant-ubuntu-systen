import json
import boto3
import uuid
import base64
import time
import os
import subprocess
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook
import sagemaker
from sagemaker.tensorflow import TensorFlow
from sagemaker.tensorflow import TensorFlowModel
from sagemaker.xgboost.estimator import XGBoost
from sagemaker.xgboost.model import XGBoostModel
from sagemaker.pytorch.model import PyTorchModel
from sagemaker.local import LocalSession
from sagemaker.session import Session

class SageMakerOperatorBuilder:

    def callableFunction(self,taskid,states,context,DYNMODB_TABLE_NAME,workflow_execution_id,**kwargs):
        s3client = boto3.client('s3')
        region = s3client.meta.region_name
        print('region is',region)

        dynamodb = boto3.resource('dynamodb',region)
        ssmclient = boto3.client('ssm',region)
        dynamodbtable = ssmclient.get_parameter(
            Name='DYNMODB_TABLE'
        )['Parameter']['Value']
        table = dynamodb.Table(dynamodbtable)
        key = {
            "id": int(workflow_execution_id),
            "type": "workflowExecution"
        }
        tasksarns = []
        responsedb = table.get_item(Key=key)['Item']
        if(responsedb.get('tags')):
            for items in responsedb.get('tags'):
                tasksarns.append(items)
        print(tasksarns)

        taskname = taskid.replace('_',' ')
        parameters = states.get(taskname).get('Parameters')

        transformjobname = 'BatchInference-'+str(uuid.uuid1())
        client = boto3.client('sagemaker',region_name=region)

        tasksarns.append(transformjobname+"_DeploymentSAGEMAKER")
        print(tasksarns)
        table.update_item(
            Key={"id": workflow_execution_id, "type": "workflowExecution"},
            UpdateExpression='SET tags = :list',
            ExpressionAttributeValues={':list': tasksarns},
            ReturnValues="UPDATED_NEW"
        )

        extras = parameters['Tags']
        print("extras", extras)
        for i in extras:
            if i['Key']=='type':
                Type = i['Value']

        if Type=='framework':
            for i in extras:
                if i['Key']=='framework':
                    frameworkType = i['Value']
                elif i['Key']=='model_s3_location':
                    model_s3_location = i['Value']
                elif i['Key']=='inference_code':
                    inference_code = i['Value']
                elif i['Key']=='role':
                    role = i['Value']

            input_data_path = parameters.get('TransformInput').get('DataSource').get('S3DataSource').get('S3Uri')
            output_data_path = parameters.get('TransformOutput').get('S3OutputPath')
            # download inference script to local
            new_path = '/mnt/dags/sagemaker/{}/{}'.format(context['executionId'],'inference.py')
            cmd='aws s3 cp {0} {1}'.format(inference_code,new_path)
            out=subprocess.check_output(cmd,shell=True)
            inference_script_location = new_path
            # fetch instance details
            instance_count = parameters.get('TransformResources').get('InstanceCount')
            instance_type = parameters.get('TransformResources').get('InstanceType')
            # fetch input parameters
            content_type = parameters.get('TransformInput').get('ContentType')
            split_type = parameters.get('TransformInput').get('SplitType')
            # fetch output parameters
            assemble_with = parameters.get('TransformOutput').get('AssembleWith')
            accept = parameters.get('TransformOutput').get('Accept')

            if frameworkType=="pytorch":
                pytorch_model = PyTorchModel(
                                    model_data=model_s3_location,
                                    role=role,
                                    framework_version="1.8.0",
                                    py_version="py3",
                                    entry_point=inference_script_location
                                    )

                transformer = pytorch_model.transformer(
                                                instance_count=instance_count,
                                                instance_type=instance_type,
                                                max_concurrent_transforms=32,
                                                max_payload=1,
                                                output_path=output_data_path,
                                                assemble_with = assemble_with,
                                                accept = accept
                                                )
                transformer.transform(
                        job_name=transformjobname,
                        data=input_data_path,
                        data_type="S3Prefix",
                        content_type=content_type
                        )

                transformer.wait()

                transformer.delete_model()

            elif frameworkType=="xgboost":
                xgboost_model = XGBoostModel(
                                    model_data=model_s3_location,
                                    role=role,
                                    entry_point=new_path,
                                    framework_version="1.0-1",
                                    env={'MMS_DEFAULT_RESPONSE_TIMEOUT': '500'},
                                    py_version='py3'
                                    )

                transformer = xgboost_model.transformer(
                            instance_count=instance_count,
                            instance_type=instance_type,
                            max_concurrent_transforms=32,
                            max_payload=1,
                            output_path = output_data_path,
                            assemble_with = assemble_with,
                            accept = accept                            )

                transformer.transform(
                    job_name=transformjobname,
                    data=input_data_path,
                    data_type="S3Prefix",
                    content_type=content_type
                        )

                transformer.wait()

                transformer.delete_model()

            elif frameworkType=="tensorflow":

                tensorflow_model = TensorFlowModel(
                                    model_data=model_s3_location,
                                    role=role,
                                    framework_version="1.13",
                                    #py_version="py3",
                                    entry_point=inference_script_location
                                    )

                transformer = tensorflow_model.transformer(
                                                instance_count=instance_count,
                                                instance_type=instance_type,
                                                max_concurrent_transforms=32,
                                                max_payload=1,
                                                output_path=output_data_path,
                                                assemble_with = assemble_with,
                                                accept = accept
                                                )
                transformer.transform(
                        job_name=transformjobname,
                        data=input_data_path, 
                        data_type="S3Prefix",
                        content_type=content_type
                        )

                transformer.wait()

                transformer.delete_model()

            elif frameworkType=="mxnet":
                mxnet_model = MXNetModel(
                                    model_data=model_s3_location,
                                    role=role,
                                    framework_version="1.6.0",
                                    py_version="py3",
                                    entry_point=inference_script_location
                                    )

                transformer = mxnet_model.transformer(
                                                instance_count=instance_count,
                                                instance_type=instance_type,
                                                max_concurrent_transforms=32,
                                                max_payload=1,
                                                output_path=output_data_path,
                                                assemble_with = assemble_with,
                                                accept = accept
                                                )
                transformer.transform(
                        job_name=transformjobname,
                        data=input_data_path,
                        data_type="S3Prefix",
                        content_type=content_type
                        )

                transformer.wait()

                transformer.delete_model()

            elif frameworkType=="chainer":

                chainer_model = ChainerModel(
                                    model_data=model_s3_location,
                                    role=role,
                                    framework_version="5.0.0",
                                    py_version="py3",
                                    entry_point=inference_script_location
                                    )

                transformer = chainer_model.transformer(
                                                instance_count=instance_count,
                                                instance_type=instance_type,
                                                max_concurrent_transforms=32,
                                                max_payload=1,
                                                output_path=output_data_path,
                                                assemble_with = assemble_with,
                                                accept = accept
                                                )
                transformer.transform(
                        job_name=transformjobname,
                        data=input_data_path,
                        data_type="S3Prefix",
                        content_type=content_type
                        )

                transformer.wait()

                transformer.delete_model()

            elif frameworkType=="scikitlearn":

                sklearn_model = SKLearnModel(
                                    model_data=model_s3_location,
                                    role=role,
                                    framework_version="0.20.0",
                                    py_version="py3",
                                    entry_point=inference_script_location
                                    )

                transformer = chainer_model.transformer(
                                                instance_count=instance_count,
                                                instance_type=instance_type,
                                                max_concurrent_transforms=32,
                                                max_payload=1,
                                                output_path=output_data_path,
                                                assemble_with = assemble_with,
                                                accept = accept
                                                )
                transformer.transform(
                        job_name=transformjobname,
                        data=input_data_path,
                        data_type="S3Prefix",
                        content_type=content_type
                        )

                transformer.wait()

                transformer.delete_model()

        else:
            for i in extras:
                if i['Key']=='model_s3_location':
                    model_s3_location = i['Value']
                elif i['Key']=='role':
                    role = i['Value']
                elif i["Key"]=='image_url':
                    image_url = i['Value']

            response_model = client.create_model(
                    ModelName=parameters['ModelName'],
                    PrimaryContainer={
                        'Image': image_url,
                        'ImageConfig': {
                        'RepositoryAccessMode': 'Platform'
                        },
                        'Mode': 'SingleModel',
                        'ModelDataUrl': model_s3_location,
                    },
                    ExecutionRoleArn=role,
                    Tags=[{'Key':'user','Value':context['user']},
                        {'Key':'team','Value':context['team']},
                        {'Key':'department','Value':context['department']},
                        {'Key':'executionId','Value':context['executionId']}],
                    EnableNetworkIsolation=False
                )

            time.sleep(5)

            # fetch instance details
            instance_count = parameters.get('TransformResources').get('InstanceCount')
            instance_type = parameters.get('TransformResources').get('InstanceType')
            # fetch input parameters
            content_type = parameters.get('TransformInput').get('ContentType')
            split_type = parameters.get('TransformInput').get('SplitType')
            # fetch output parameters
            assemble_with = parameters.get('TransformOutput').get('AssembleWith')
            accept = parameters.get('TransformOutput').get('Accept')

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
                    'ContentType': content_type,
                    'CompressionType': 'None',
                    'SplitType': split_type
                },
                TransformOutput={
                    'S3OutputPath': parameters.get('TransformOutput').get('S3OutputPath'),
                    'Accept': accept,
                    'AssembleWith': assemble_with
                },
                TransformResources={
                    'InstanceType': instance_type,
                    'InstanceCount': instance_count
                },
                DataProcessing={
                    'InputFilter': '$',
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

    def getOperator(self, dag, task_id, states, context, DYNMODB_TABLE_NAME,workflow_execution_id):
        operator = PythonOperator(
        task_id = task_id,
        provide_context=True,
        python_callable = SageMakerOperatorBuilder().callableFunction,
        op_kwargs={'taskid': task_id, 'states': states, 'context': context, "DYNMODB_TABLE_NAME": DYNMODB_TABLE_NAME,'workflow_execution_id':workflow_execution_id},
        dag = dag,
        )
        return operator