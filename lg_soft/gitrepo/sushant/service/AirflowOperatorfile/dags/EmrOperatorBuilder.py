import os
import subprocess
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

def create_emr_resource(region, create_resource_emr_, encoded, context, DYNMODB_TABLE_NAME):

    dynamodb = boto3.resource('dynamodb')
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
        print(update_db_table)

    except:
        update_db_table = table.update_item(
                    Key={"id": workflow_execution_id, "type": "workflowExecution"},
                    UpdateExpression='SET workflowVersionCFNTemplate = :list',
                    ExpressionAttributeValues={':list': [stack_name]},
                    ReturnValues="UPDATED_NEW"
                )
        print(update_db_table)
    
    hook = AwsLambdaHook(create_resource_emr_,
                        region_name=region,
                        log_type='None',
                        qualifier='$LATEST',
                        invocation_type='RequestResponse',
                        config=None,
                        aws_conn_id='aws_default')

    payload = '{"stackName": "%s", "cft_template": "%s" }' % (stack_name, encoded)
    response_1 = hook.invoke_lambda(payload=payload)

    time.sleep(120)

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

def delete_emr_resource(region, stack_name, delete_resource_emr_):

    stack_name = stack_name
    Function_name = delete_resource_emr_
    hook = AwsLambdaHook(Function_name,
                         region_name=region,
                         qualifier='$LATEST',
                         invocation_type='RequestResponse',
                         aws_conn_id='aws_default')
    payload = '{"stackName": "%s"}' % (stack_name)
    response_2 = hook.invoke_lambda(payload=payload)
    return response_2


class EmrOperatorBuilder:
        
    def callableFunction(self,taskid,states,context,DYNMODB_TABLE_NAME,workflow_execution_id,**kwargs):
        
        s3client = boto3.client('s3')
        region = s3client.meta.region_name
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
        create_resource_emr_ = states[task_id_def]["Parameters"]["Payload"]["resource_cft"]["create_resources_function"]
        delete_resource_emr_ = states[task_id_def]["Parameters"]["Payload"]["resource_cft"]["delete_resources_step"]
        stack_name = create_emr_resource(region, create_resource_emr_, encoded, context, DYNMODB_TABLE_NAME)
        stack_id = stack_name

        client = boto3.client('cloudformation',region_name=region)
        response = client.describe_stack_resources(StackName=stack_id, LogicalResourceId='EMRCluster')
        cluster_id = response.get('StackResources')[0].get('PhysicalResourceId')
        print(cluster_id)
        taskname = taskid.replace('_',' ')
        stepName = json.dumps(states.get(taskname).get('Parameters').get('stepName'))
        Script = states[task_id_def]["Parameters"]["Payload"]["inputpyfile"]
        
        try:
            script_location = os.path.split(Script)
            s3_location = script_location[0]
            name_bucket = s3_location.split('/')[2]
            name_key = '/'.join(s3_location.split('/')[3:])
            path = "/root/airflow/emr/execution-"+context['executionId']+"-"+str(uuid.uuid4())[:8]
            cmd="mkdir -p {}".format(path)
            out=subprocess.check_output(cmd,shell=True)
            s3_client = boto3.resource('s3')
            bucket = s3_client.Bucket(name_bucket)
            folder=name_key.split('/')[-1]
            for obj in bucket.objects.filter(Prefix = name_key):
                new_path=path
                full_path=obj.key.split('/')
                for i in range(len(full_path)):
                    if full_path[i] == folder:
                        k=i+1
                        break
                for i in range(k,len(full_path)-1):
                    new_path = new_path+"/"+full_path[i]
                print("new_path",new_path)
                cmd="mkdir -p "+new_path
                os.system(cmd)
                name=obj.key.split('/')[-1]
                new_path=new_path+"/"+name
                bucket.download_file(obj.key, new_path)
            primary_zip = "support-"+context['executionId']+".zip"
            cmd="cd "+path+"&& "+"zip -r "+primary_zip+" *"
            out=subprocess.check_output(cmd,shell=True)
            zip_location=path+"/"+primary_zip
            s3__client = boto3.client('s3')
            support_object_name ='public/Workflow/ExecutionFiles/{0}/{1}'.format(context['executionId'],'support.zip')
            response = s3__client.upload_file(zip_location, name_bucket, support_object_name)
            cmd="rm -rf "+path
            out=subprocess.check_output(cmd,shell=True)

            CODE_DIR = states[taskname]['Parameters']['Payload']['command']
            argument = CODE_DIR[0]
            suport_location = "s3://{0}/{1}".format(name_bucket,support_object_name)
            argument.insert(0, suport_location)
            argument.insert(0, "--py-files")
            argument.insert(0, "/usr/bin/spark-submit")
            argument.insert(0, "sudo")

            emr = boto3.client('emr', region_name=region)
            step = {"Name": stepName,
                   'ActionOnFailure': 'CONTINUE',
                   'HadoopJarStep': {
                        'Jar': '/usr/share/aws/emr/command-runner/lib/command-runner-0.1.0.jar',
                        'Args': argument
                    }
                }
            action = emr.add_job_flow_steps(JobFlowId= cluster_id, Steps=[step])
            print("Added step: %s"%(action))
            stepid = action.get('StepIds')
        
        except:
            CODE_DIR = states[taskname]['Parameters']['Payload']['command']
            argument = CODE_DIR[0]
            argument.insert(0, "/usr/bin/spark-submit")
            argument.insert(0, "sudo")
            emr = boto3.client('emr', region_name=region)
            step = {"Name": stepName,
                   'ActionOnFailure': 'CONTINUE',
                   'HadoopJarStep': {
                        'Jar': '/usr/share/aws/emr/command-runner/lib/command-runner-0.1.0.jar',
                        'Args': argument
                    }
                }
            action = emr.add_job_flow_steps(JobFlowId= cluster_id, Steps=[step])
            print("Added step: %s"%(action))
            stepid = action.get('StepIds')

        tasksarns.append(cluster_id+"__stepid__"+stepid[0]+"_DeploymentEMR")
        print(tasksarns)
        responsedb['tags']=tasksarns
        print(responsedb)
        table.put_item(Item=responsedb)

        while True:
            for steps in stepid:
                response = emr.describe_step(
                    ClusterId=cluster_id,
                    StepId=steps
                )
            status = response.get('Step').get('Status').get('State')
            print(status)
            time.sleep(5)
            if status =='COMPLETED': # or status =='RUNNING':
                delete_emr_resource(region, stack_name, delete_resource_emr_)
                break
            elif status == 'FAILED':
                delete_emr_resource(region, stack_name, delete_resource_emr_)
                raise ValueError('Job run fail on EMR')

        print("execution is complete")

        x = CODE_DIR[0]
        a = x.index('--outputfile')
        b = a+1
        s3_location = x[b]

        name_bucket = s3_location.split('/')[2]
        name_key = '/'.join(s3_location.split('/')[3:])

        client = boto3.client('s3')
        s3 = boto3.resource('s3')
        my_bucket = s3.Bucket(name_bucket)
        s3_key = '/'.join(name_key)

        for object_summary in my_bucket.objects.filter(Prefix=name_key):
            a = object_summary.key
            result = a.startswith(name_key+"/part")#(name_key+'/part')
            if result==True:
                file_name = a
                print(file_name)


        copy_source = {
            'Bucket': name_bucket,
            'Key': file_name
        }

        s3.meta.client.copy(copy_source, name_bucket , name_key) 

    def getOperator(self, dag, task_id, states, context, DYNMODB_TABLE_NAME, workflow_execution_id):
        operator = PythonOperator(
	    task_id = task_id,
	    provide_context=True,
	    python_callable = EmrOperatorBuilder().callableFunction,
            op_kwargs={'taskid': task_id, 'states': states, 'context': context, "DYNMODB_TABLE_NAME": DYNMODB_TABLE_NAME, 'workflow_execution_id':workflow_execution_id},
	    dag = dag,
	    )
        return operator
