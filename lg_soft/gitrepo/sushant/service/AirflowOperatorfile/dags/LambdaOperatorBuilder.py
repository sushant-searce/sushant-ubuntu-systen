import subprocess
import os
import uuid
import yaml
import json
import boto3
import uuid
import base64
import time
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook

def create_lambda_resource(region, create_resource_lambda_, encoded, context, DYNMODB_TABLE_NAME):
    dynamodb = boto3.resource('dynamodb', region)
    dynamodb_table_name = DYNMODB_TABLE_NAME
    table = dynamodb.Table(dynamodb_table_name)
    workflow_execution_id = int(context['executionId'])
    stack_name = 'cft-' + str(uuid.uuid4())[:8]
    
    try:
        update_db_table = table.update_item(
                Key={"id": workflow_execution_id, "type": "workflowExecution"},
                UpdateExpression="SET workflowVersionCFNTemplate = list_append(workflowVersionCFNTemplate, :list)",
                ExpressionAttributeValues={':list': [stack_name]},
                ReturnValues="UPDATED_NEW"
            )
    except:
        update_db_table = table.update_item(
                    Key={"id": workflow_execution_id, "type": "workflowExecution"},
                    UpdateExpression='SET workflowVersionCFNTemplate = :list',
                    ExpressionAttributeValues={':list': [stack_name]},
                    ReturnValues="UPDATED_NEW"
                )

    hook = AwsLambdaHook(create_resource_lambda_,
                        region_name=region,
                        log_type='None',
                        qualifier='$LATEST',
                        invocation_type='RequestResponse',
                        config=None,
                        aws_conn_id='aws_default')
    payload = '{"stackName": "%s", "cft_template": "%s" }' % (stack_name, encoded)
    response_1 = hook.invoke_lambda(payload=payload)
    time.sleep(10)

    while True:
        client = boto3.client('cloudformation' ,region_name=region)
        response = client.describe_stacks(StackName= stack_name)
        stack_status = response.get('Stacks')[0].get('StackStatus')
        time.sleep(3)
        if stack_status == 'CREATE_COMPLETE':
            print('Creating resources is in progress')
            break
        elif stack_status=='CREATE_FAILED' or stack_status=="ROLLBACK_IN_PROGRESS" or stack_status=="ROLLBACK_FAILED" or stack_status=="ROLLBACK_COMPLETE":
            raise ValueError('Create resource stack failed')
    
    return stack_name


def delete_lambda_resource(region, stack_name, delete_resource_lambda_):
    stack_name = stack_name
    Function_name = delete_resource_lambda_
    hook = AwsLambdaHook(Function_name,
                         region_name=region,
                         qualifier='$LATEST',
                         invocation_type='RequestResponse',
                         aws_conn_id='aws_default')
    payload = '{"stackName": "%s"}' % (stack_name)
    response_2 = hook.invoke_lambda(payload=payload)
    return response_2

class LambdaOperatorBuilder:
    
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
        task_id_def = taskid.replace("_", " ")
        encoded = states[task_id_def]["Parameters"]["Payload"]["resource_cft"]["cft_template"]
        create_resource_lambda_ = states[task_id_def]["Parameters"]["Payload"]["resource_cft"]["create_resources_function"]
        delete_resource_lambda_ = states[task_id_def]["Parameters"]["Payload"]["resource_cft"]["delete_resources_step"]
        stack_name = create_lambda_resource(region, create_resource_lambda_, encoded, context, DYNMODB_TABLE_NAME)
        stack_id = stack_name
        state_name = taskid.replace('_',' ')
        logicalid = states[state_name]['Parameters']['FunctionName.$']
        logicalid = logicalid.replace(' ','')
        client = boto3.client('cloudformation',region_name=region)
        response = client.describe_stack_resources(StackName=stack_id,LogicalResourceId=logicalid)
        x=response.get('StackResources')
        
        for i in x:
            fn_name=i.get('PhysicalResourceId')
        lambda_client = boto3.client('lambda',region_name=region)         
        
        folder= "/root/airflow/dags/execution"+str(uuid.uuid4())[:8]
        cmd="mkdir -p " + folder
        os.system(cmd)
        config = states[state_name].get("Parameters").get("Payload").get('config_string')
        value =  yaml.safe_load(config)
        run_time = value.get('deployment').get('config').get('runTime')        
        artifact_location = states[state_name].get("Parameters").get("Payload").get('artifact_location')
        extra_zip_location = states[state_name].get("Parameters").get("Payload").get("extra").get('zip_location_key')
        if artifact_location.startswith('s3://'):
            artifact_location = artifact_location[5:]
            s3_components = artifact_location.split('/')
            bucket_name = s3_components[0]
            key=""
            if len(s3_components)>1:
                key='/'.join(s3_components[1:])
        path=key + "/requirements.txt"
        s3_client = boto3.client('s3')
        local_path=folder + "/requirements.txt"
        file = s3_client.download_file(bucket_name, path, local_path)
        path2=key + "/commands.txt"
        local_path2 = folder + "/commands.txt"
        file = s3_client.download_file(bucket_name, path2, local_path2)
        
        python_path = folder +"/python"
        cmd="mkdir -p " + python_path 
        os.system(cmd)
        cmd="mkdir -p " + python_path+"/generic/"
        os.system(cmd)
        run="cd /root/.pyenv/shims/ && "+"./"+run_time
        cmd=run+" -m  pip install -r " + local_path
        print(cmd)
        out=subprocess.check_output(cmd,shell=True)
        cmd=run+" -m  pip install -r "+local_path + " -t " + python_path
        out=subprocess.check_output(cmd,shell=True)
        command_file = open(local_path2, 'r')
        Lines = command_file.readlines()
        for line in Lines:
            line=line.replace("python",run).replace("\n","")
            cmd=line + " -d " + python_path+ "/generic/"
            out=subprocess.check_output(cmd,shell=True)
        zip_name="req_layer_" + str(uuid.uuid4())[:8] + ".zip"
        cmd = "cd " + folder + "&& zip -r " + zip_name + " python/"
        subprocess.check_output(cmd,shell=True)
        file_name = folder + '/' + zip_name
        bucket = bucket_name
        object_name ='lambda_zip_scripts/'+zip_name
        response = s3_client.upload_file(file_name, bucket, object_name)
        cmd="rm -rf "+folder
        os.system(cmd)

        layer_name="layer-lambda"+str(uuid.uuid4())[:8]
        response = lambda_client.publish_layer_version(
                LayerName=layer_name,
                Content={
                    'S3Bucket': bucket,
                    'S3Key': object_name
                    },
                CompatibleRuntimes=[run_time]
                )   
        layer_arn = response.get('LayerArn')
        layer_version=response.get('Version')
        layer_version_arn = response.get('LayerVersionArn')
                
        response = lambda_client.update_function_configuration(
                FunctionName = fn_name,
                Layers=[layer_version_arn]
            )

        hook = AwsLambdaHook(fn_name,
                        region_name=region,
                        invocation_type='RequestResponse',
                        aws_conn_id='aws_default')

        taskname = taskid.replace('_',' ') 
        payload = json.dumps(states.get(taskname).get('Parameters').get('Payload'))
        response_2 = hook.invoke_lambda(payload=payload)

        tasksarns.append(fn_name+"_DeploymentLAMBDA")
        print(tasksarns)
        responsedb['tags']=tasksarns
        print(responsedb)
        table.put_item(Item=responsedb)

        response = lambda_client.delete_layer_version(
            LayerName=layer_arn,
            VersionNumber=layer_version
            )

        #deleting the layer zip
        response = s3_client.delete_object(
            Bucket=bucket,
            Key = object_name
            )
        
        #deleting the script zip
        response = s3_client.delete_object(
            Bucket = bucket,
            Key = extra_zip_location
            )
        
    
        delete_lambda_resource(region, stack_name, delete_resource_lambda_)
        
        return "completed" 
	
    def getOperator(self, dag, task_id, states,context, DYNMODB_TABLE_NAME, workflow_execution_id):
        operator = PythonOperator(
	    task_id = task_id,
	    provide_context=True,
	    python_callable = LambdaOperatorBuilder().callableFunction,
        op_kwargs={'taskid': task_id, 'states': states, 'context': context, "DYNMODB_TABLE_NAME": DYNMODB_TABLE_NAME, 'workflow_execution_id':workflow_execution_id},
	    dag = dag,
	)
        return operator
