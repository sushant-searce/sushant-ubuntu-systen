import uuid
import base64
import os
import boto3
import constants

def dag(workflow_execution_id, context):

    tag_context = context['tags']

    definition = """import flask
import yaml
import json
import boto3
import uuid
import base64
import time
from airflow import DAG
from flask import request
from airflow.models import DagRun
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook
from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook
from modules.LambdaOperatorBuilder import LambdaOperatorBuilder
from modules.ECSOperatorBuilder import ECSOperatorBuilder
from modules.ECSEC2OperatorBuilder import ECSEC2OperatorBuilder
from modules.GlueOperatorBuilder import GlueOperatorBuilder
from modules.EmrOperatorBuilder import EmrOperatorBuilder
from modules.SageMakerOperatorBuilder import SageMakerOperatorBuilder
from airflow.operators.python_operator import BranchPythonOperator

context = CONTEXT

DYNMODB_TABLE_NAME = "DYNAMODB_TABLE"

def read_deployment_yaml():
    file_name= "public/airflow/definationyaml/searceLG/definition.yaml"
    bucket = 'WORKFLOW_EXECUTION_YAML_BUCKET'
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket, Key=file_name)
    deployment = yaml.safe_load(response["Body"])
    return deployment

s3client = boto3.client('s3')
region = s3client.meta.region_name
updated_definition = read_deployment_yaml()

start = updated_definition.get('StartAt')
states = updated_definition.get('States')
delete_output_list=[]
visited=[]
task_list=[]

function_name='PROJECT_COMPLETION_FUNCTION_ARN'

def choice_function(choice_args):
    #time.sleep(40)
    s3 = boto3.resource('s3')
    bucketname = choice_args.get('status_bucket_name')
    itemname = choice_args.get('status_file_name')
    bucket = s3.Bucket(bucketname)
    obj = bucket.Object(key=itemname)
    body = obj.get()['Body'].read()
    body = body.decode("utf-8")
    #status = body.split(":")[1]
    status = body
    status = status.replace("\\n","").replace(" ","")
    conditions=choice_args.get('conditions')
    for condition in conditions:
        task_id = condition.get('task_id')
        expression = condition.get('expression')
        expression_value = condition.get('expression_value')
        if expression == 'NumericEquals':
            if expression_value == int(status):
                return task_id
        elif expression == 'NumericGreaterThan':
            if expression_value < int(status):
                return task_id
        elif expression == 'NumericLessThan':
            if expression_value > int(status):
                return task_id
        elif expression == 'StringEquals':
            if expression_value == status:
                return task_id
        elif expression == 'StringGreaterThan':
            if expression_value < status:
                return task_id
        elif expression == 'StringLessThan':
            if expression_value > status:
                return task_id
    return None

def create_choice_operator(task_id, choice_args):
    branch_op = BranchPythonOperator(
        task_id=task_id,
        python_callable=choice_function,
        op_kwargs={'choice_args': choice_args},
        dag=dag,
    )
    return branch_op

def create_dynamic_operator(task_id, callableFunction):
    task = PythonOperator(
        task_id=task_id,
        provide_context=True,
        python_callable=eval(callableFunction),
        on_failure_callback=on_failure_callback,
        dag=dag,
    )
    return task


def on_failure_callback(context):

    dag_run = context.get('dag_run')
    task_instances = dag_run.get_task_instances()

    hook1 = AwsLambdaHook(function_name,
                         region_name=region,
                         qualifier='$LATEST',
                         invocation_type='RequestResponse',
                         aws_conn_id='aws_default')

    payload1 = '{"workflow_execution_id" : "searceLG", "execution_status" : "FAILED"}'
    response_3 = hook1.invoke_lambda(payload=payload1)
    workflow_exec_id_ = "searceLG"
    s3client = boto3.client('s3')
    region = s3client.meta.region_name
    dynamodb = boto3.resource('dynamodb')
    dynamodb_table_name = DYNMODB_TABLE_NAME
    table = dynamodb.Table(dynamodb_table_name)
    response = table.get_item(Key={"id": int(workflow_exec_id_), "type": "workflowExecution"})
    stack_name = response['Item']['workflowVersionCFNTemplate']
    client = boto3.client('cloudformation', region)

    for i in stack_name:
        try:
            response = client.delete_stack(StackName=i)
            print("stack name:", i)
            print(response)
        except:
            pass
    
args = {
    'owner': 'airflow',
    'email': ['nigud.sushant@gmail.com'],
    'provide_context': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'on_failure_callback': on_failure_callback,
}

dag = DAG('LG-dynamic-searceLG',
        description='testing unit module example',
        schedule_interval='@once',
        start_date=datetime.now(),
        on_failure_callback=on_failure_callback,
        catchup=False,
        default_args=args)

def update_stopped_status(**kwargs):

    hook = AwsLambdaHook(function_name,
                         region_name=region,
                         qualifier='$LATEST',
                         invocation_type='RequestResponse',
                         aws_conn_id='aws_default')

    payload = '{"workflow_execution_id" : "searceLG", "execution_status" : "COMPLETED"}'
    response_3 = hook.invoke_lambda(payload=payload)
    print(response_3)
    delete_s3_outputs()

start_worflow = DummyOperator(task_id='start', retries = 3, dag=dag)
end_workflow = PythonOperator(task_id='end', python_callable=update_stopped_status, dag=dag)

def create_dynamic_module(task_type, task_id, resource_type, states):
    _state_name = task_id.replace("_"," ")
    if task_type == "Task":
        if resource_type == 'arn:aws:states:::lambda:invoke':
            dynamicTask = LambdaOperatorBuilder().getOperator(dag, task_id, states, context, DYNMODB_TABLE_NAME, searceLG)
        elif resource_type == 'arn:aws:states:::ecs:runTask.sync':
            #for ecs-ec2 and fargate
            parameters = states[_state_name].get("Parameters").get("Overrides").get("ContainerOverrides")
            for item in parameters:
                if item.get("Name") == "container1":
                    environments = item.get("Environment")
                    break
            for env in environments:
                if env.get('Name') == 'CONFIG_FILE':
                    value = env.get('Value')
                    break
            value =  yaml.safe_load(value)
            ecs_type = value.get('deployment').get('type')
            print(ecs_type)
            if ecs_type =='ecs':
                dynamicTask = ECSOperatorBuilder().getOperator(dag, task_id, states, searceLG)
            elif ecs_type=='ecs-ec2':
                dynamicTask = ECSEC2OperatorBuilder().getOperator(dag, task_id, states, searceLG, DYNMODB_TABLE_NAME)
        elif resource_type == 'arn:aws:states:::sagemaker:createTransformJob.sync':
            dynamicTask = SageMakerOperatorBuilder().getOperator(dag, task_id, states, context, DYNMODB_TABLE_NAME)
        elif resource_type == 'arn:aws:states:::elasticmapreduce:addStep.sync':
            dynamicTask = EmrOperatorBuilder().getOperator(dag, task_id, states, context, DYNMODB_TABLE_NAME, searceLG)
        elif resource_type == 'arn:aws:states:::glue:startJobRun.sync':
            dynamicTask = GlueOperatorBuilder().getOperator(dag, task_id, states, context, DYNMODB_TABLE_NAME, searceLG)
            # print('notify in parallel')
            # dynamicTask = task_id
    else:
        dynamicTask = create_dynamic_operator('{}'.format(task_id), '{}'.format(task_id))
    return (dynamicTask)

def get_file_location(task_type, task_id, resource_type, states):
    _state_name = task_id.replace("_"," ")
    status_file_path="success"
    if task_type == "Task":
        if resource_type == 'arn:aws:states:::lambda:invoke':
            parameters = states[_state_name].get("Parameters").get("Payload").get("output").get("dest")
            for parameter in parameters:
                if parameter.get('status') == True:
                    status_file_path = parameter.get('filePath')
                    break
        elif resource_type == 'arn:aws:states:::sns:publish':
            pass
        elif resource_type == 'arn:aws:states:::ecs:runTask.sync':
            #for ECS
            parameters = states[_state_name].get("Parameters").get("Overrides").get("ContainerOverrides")
            for item in parameters:
                if item.get("Name") == "container1":
                    environments = item.get("Environment")
                    break
            for env in environments:
                if env.get('Name') == 'CONFIG_FILE':
                    value = env.get('Value')
                    break
            value =  yaml.safe_load(value)
            outputs = value.get('output').get('dest')
            for output in outputs:
                if output.get('status') == True:
                    status_file_path = output.get('filePath')
                    break
        elif resource_type == 'arn:aws:states:::sagemaker:createTransformJob.sync':
            #dependency on shashi
            pass
        elif resource_type == 'arn:aws:states:::elasticmapreduce:addStep.sync':
            # For EMR
            parameters = states[_state_name].get("Parameters").get("Payload").get("dest")
            for parameter in parameters:
                if parameter.get('status') == True:
                    status_file_path = parameter.get('filePath')
                    break
        elif resource_type == 'arn:aws:states:::glue:startJobRun.sync':
            # For Glue
            parameters = states[_state_name].get("Parameters").get("Payload").get("dest")
            for parameter in parameters:
                if parameter.get('status') == True:
                    status_file_path = parameter.get('filePath')
                    break
    else:
        pass
    return status_file_path

def get_save_skip_status(task_type, task_id, resource_type, states):
    _state_name = task_id.replace("_"," ")
    skip="no"
    global delete_output_list
    if task_type == "Task":
        if resource_type == 'arn:aws:states:::lambda:invoke':
            parameters = states[_state_name].get("Parameters").get("Payload").get('extra')
            print("para",parameters)
            save = parameters.get('saveOutput')
            skip = parameters.get('skipModule')
            print(save,skip)
            if save == 'no':
                parameters = states[_state_name].get("Parameters").get("Payload").get("output").get("dest")
                for item in parameters:
                    value=item.get('filePath')
                    delete_output_list.append(value) 
        elif resource_type == 'arn:aws:states:::sns:publish':
            pass
        elif resource_type == 'arn:aws:states:::ecs:runTask.sync':
            #for ECS
            parameters = states[_state_name].get("Parameters").get('tags')
            for parameter in parameters:
                if parameter.get('key')=='extra':
                    values=parameter.get('value')
                    break
            save=values.get('saveOutput')
            skip=values.get('skipModule')
            if save=='no':
                parameters = states[_state_name].get("Parameters").get("Overrides").get("ContainerOverrides")
                for item in parameters:
                    if item.get("Name") == "container3":
                        environments = item.get("Environment")
                        for env in environments:
                            values=env.get("Value").split(" ")
                            for value in values:
                                delete_output_list.append(value)        
        elif resource_type == 'arn:aws:states:::sagemaker:createTransformJob.sync':
            #dependency on shashi 
            pass
        elif resource_type == 'arn:aws:states:::elasticmapreduce:addStep.sync':
            # For EMR
            parameters = states[_state_name].get("Parameters").get("Payload").get('extra')
            print("para",parameters)
            save = parameters.get('saveOutput')
            skip = parameters.get('skipModule')
            print(save,skip)
            if save == 'no':
                parameters = states[_state_name].get("Parameters").get("Payload").get("dest")
                print("second",parameters)
                for parameter in parameters:
                    delete_output_list.append(parameter.get('filePath'))         

        elif resource_type == 'arn:aws:states:::glue:startJobRun.sync':
            # For Glue
            parameters = states[_state_name].get("Parameters").get("Payload").get("extra")
            save = parameters.get('saveOutput')
            skip = parameters.get('skipModule')
            if save == "no":
                parameters = states[_state_name].get("Parameters").get("Payload").get("dest")
                for parameter in parameters:
                    delete_output_list.append(parameter.get('filePath'))
    else:
        pass
    return skip

def delete_s3_outputs():
    global delete_output_list
    s3 = boto3.resource('s3')
    for file_path in delete_output_list:
        if file_path.startswith('s3://'):
            file_path=file_path[5:]
            s3_components = file_path.split('/')
            bucket = s3_components[0]
            key=""
            if len(s3_components)>1:
                key='/'.join(s3_components[1:])
        print("bucket:",bucket)
        print("key:",key)
        s3.Object(bucket, key).delete()

def create_dynamic_task(state_name, states, upstream_task, downstream_task):
    states_length=len(states)
    for _state_name in states:
        if _state_name == state_name:
            state = states.get(_state_name)
            task_id = _state_name.replace(' ', '_')
            break
    if state_name == None:
        return
    resource_type = state.get('Resource')
    task_type = state.get('Type')
    next_state_name = state.get('Next')
    end_state = state.get('End')
    if task_type == 'Parallel':
        if end_state == True:
            print("parallel state end is true")
            pass
        else:
            downstream_task = create_dynamic_task(next_state_name, states, None, downstream_task)
        branches = state.get('Branches')
        for branch in branches:
            create_dynamic_branch(branch, upstream_task, downstream_task)
    elif task_type == 'Choice':
        if end_state == True:
            print("choice state end is true")
            pass
        else:
            next_state_name = state.get('Default')
            if next_state_name!=None:
                downstream_task = create_dynamic_task(next_state_name, states, None, downstream_task)
            downstream_task.trigger_rule = "none_failed_or_skipped"
            choices = state.get('Choices')
            choice_args = {}
            conditions = []
            choice_tasks = []
            is_custom_code = False
            for choice in choices:
                variable = choice.get("Variable")
                expression = list(choice)[1]
                expression_value = choice.get(expression)
                next_state_name = choice.get('Next')
                if 'StatusCode' in variable:
                    if expression_value == 200:
                        choice_task = create_dynamic_task(next_state_name, states, upstream_task, downstream_task)
                        choice_task.trigger_rule = "one_success"
                        print("condition pass")
                    else:
                        choice_task = create_dynamic_task(next_state_name, states, upstream_task, downstream_task)
                        choice_task.trigger_rule = "one_failed"
                        print("condition fail")
                else:
                    is_custom_code = True
                    choice_task = create_dynamic_task(next_state_name, states, None, downstream_task)
                    condition = {'expression': expression, 'expression_value': expression_value,
                                 'task_id': choice_task.task_id}
                    conditions.append(condition)
                    choice_tasks.append(choice_task)
                    print("condition {}".format(expression_value))
            if (is_custom_code):
                choice_args['conditions'] = conditions
                for k in range(states_length):
                    if state_name == list(states)[k]:
                        previous_state = list(states)[k-1]
                        break
                previous_resource_type = states[previous_state].get('Resource')
                previous_task_type = states[previous_state].get('Type')
                file_path = get_file_location(previous_task_type, previous_state, previous_resource_type, states)
                print("previous_state:",previous_state)
                #file_path=choice_status[previous_state]
                print("status_file_path:",file_path)
                if file_path.startswith('s3://'):
                    file_path=file_path[5:]
                    s3_components = file_path.split('/')
                    bucket = s3_components[0]
                    key=""
                    if len(s3_components)>1:
                        key='/'.join(s3_components[1:])
                print("bucket:",bucket)
                print("key:",key)
                choice_args['status_bucket_name'] = bucket
                choice_args['status_file_name'] = key
                choice_operator_task = create_choice_operator(task_id, choice_args)
                upstream_task.set_downstream(choice_operator_task)
                for _choice_task in choice_tasks:
                    choice_operator_task.set_downstream(_choice_task)
    else:
        if task_id not in visited:
            visited.append(task_id)
            x=get_save_skip_status(task_type, task_id, resource_type, states)
            _dynamic_task = create_dynamic_module(task_type, task_id, resource_type, states)
            task_list.append(_dynamic_task)
        else:
            print("task_id:",task_id)
            print("upstream",upstream_task)
            print("downstream",downstream_task)
            for item in task_list:
                print("id",item.task_id)
                if task_id == item.task_id:
                    item.trigger_rule="none_failed_or_skipped"
                    break
            _dynamic_task=item
        if upstream_task != None:
            upstream_task.set_downstream(_dynamic_task)
        upstream_task = _dynamic_task
        if end_state == True:
            print("We are at end we will set downstream of upstream ", downstream_task, upstream_task)
            upstream_task.set_downstream(downstream_task)
        else:
            create_dynamic_task(next_state_name, states, upstream_task, downstream_task)
        return upstream_task


def create_dynamic_branch(branch, upstream_task, downstream_task):
    start = branch.get('StartAt')
    states = branch.get('States')
    create_dynamic_task(start, states, upstream_task, downstream_task)

create_dynamic_branch(updated_definition, start_worflow, end_workflow)""".replace('searceLG', str(workflow_execution_id)).replace("WORKFLOW_EXECUTION_YAML_BUCKET", constants.SC_S3_BUCKET).replace("CONTEXT", str(tag_context)).replace("PROJECT_COMPLETION_FUNCTION_ARN", constants.FINAL_STEP).replace("DYNAMODB_TABLE", constants.DYNMODB_TABLE)

    s3 = boto3.client('s3')
    BUCKET_NAME = constants.SC_S3_BUCKET
    file_content = definition
    file_path = "public/airflow/AirflowDagFiles/file_"+str(workflow_execution_id)+".py"

    try:
        s3_response = s3.put_object(Bucket=BUCKET_NAME, Key=file_path, Body=file_content)
    except Exception as e:
        raise IOError(e)

    print("DAG ID : LG-dynamic"+str(workflow_execution_id))

    return {
        'statusCode': 200,
        'body': {
            'file_path': file_path
        }
    }
