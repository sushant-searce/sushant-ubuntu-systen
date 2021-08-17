import flask
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
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook
from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook
from modules.LambdaOperatorBuilder import LambdaOperatorBuilder
from modules.ECSOperatorBuilder import ECSOperatorBuilder
from modules.ECSEC2OperatorBuilder import ECSEC2OperatorBuilder
from modules.NotificationOperatorBuilder import NotificationOperatorBuilder
from modules.GlueOperatorBuilder import GlueOperatorBuilder
from modules.EmrOperatorBuilder import EmrOperatorBuilder
from modules.SageMakerOperatorBuilder import SageMakerOperatorBuilder
from airflow.operators.python_operator import BranchPythonOperator
from modules.ECSEC2ALLOperatorBuilder import ECSEC2ALLOperatorBuilder

context = {'user': 'mohan.paralla', 'team': 'Proactive Customer Care Task', 'department': 'Cloud Solution Department', 'executionId': '890123082'}
def read_deployment_yaml():
    file_name= "public/airflow/definationyaml/890123082/definition.yaml"
    bucket = 'lppdatasource103424-devthree'
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
    status = status.replace("\n","").replace(" ","")
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
    stack_name = context.get("ti").xcom_pull(task_ids='Creating_Resources')
    Function_name = states.get('Deleting Resources').get('Parameters').get('FunctionName')
    hook = AwsLambdaHook(Function_name,
                         region_name=region,
                         qualifier='$LATEST',
                         invocation_type='RequestResponse',
                         aws_conn_id='aws_default')
    payload = '{"stackName": "%s"}' % (stack_name)
    response_2 = hook.invoke_lambda(payload=payload)

    hook1 = AwsLambdaHook('arn:aws:lambda:ap-northeast-2:797237262327:function:Project_Completion_Step',
                         region_name=region,
                         qualifier='$LATEST',
                         invocation_type='RequestResponse',
                         aws_conn_id='aws_default')

    payload1 = '{"workflow_execution_id" : "890123082"}'
    response_3 = hook1.invoke_lambda(payload=payload1)


def Creating_Resources(ds, **kwargs):
    encoded = states.get('Creating Resources').get('Parameters').get('Payload').get('cft_template')
    Function_name = states.get('Creating Resources').get('Parameters').get('FunctionName')
    stack_name = 'cft-' + str(uuid.uuid4())[:8]
    hook = AwsLambdaHook(Function_name,
                         region_name=region,
                         log_type='None',
                         qualifier='$LATEST',
                         invocation_type='RequestResponse',
                         config=None,
                         aws_conn_id='aws_default')
    payload = '{"stackName": "%s", "cft_template": "%s" }' % (stack_name, encoded)
    response_1 = hook.invoke_lambda(payload=payload)
    kwargs['ti'].xcom_push(key='create_resources', value=stack_name)
    return stack_name

def Wait_for_Resources_to_be_Created(ds, **kwargs):
    time.sleep(30)
    while True:
        client = boto3.client('cloudformation' ,region_name=region)
        response = client.describe_stacks(
            StackName= kwargs['ti'].xcom_pull(task_ids='Creating_Resources')
            )

        stack_status = response.get('Stacks')[0].get('StackStatus')

        time.sleep(15)
        if stack_status == 'CREATE_COMPLETE':
            print('Creating resources is in progress')
            break
        elif stack_status=='CREATE_FAILED':
            raise ValueError('Create resource stack failed')

    return "Wait Completed"

def Get_Resource_Creation_Status(ds, **kwargs):
    stack_id = kwargs['ti'].xcom_pull(task_ids='Creating_Resources')
    Function_name = updated_definition.get('States').get('Get Resource Creation Status').get('Parameters').get(
        'FunctionName')
    hook = AwsLambdaHook(Function_name,
                         region_name=region,
                         qualifier='$LATEST',
                         invocation_type='RequestResponse',
                         aws_conn_id='aws_default')
    payload = '{"stackName": "%s"}' % (stack_id)
    response_2 = hook.invoke_lambda(payload=payload)

def Deleting_Resources(ds, **kwargs):
    stack_name = kwargs['ti'].xcom_pull(task_ids='Creating_Resources')
    Function_name = states.get('Deleting Resources').get('Parameters').get('FunctionName')
    hook = AwsLambdaHook(Function_name,
                         region_name=region,
                         qualifier='$LATEST',
                         invocation_type='RequestResponse',
                         aws_conn_id='aws_default')
    payload = '{"stackName": "%s"}' % (stack_name)
    response_2 = hook.invoke_lambda(payload=payload)


args = {
    'owner': 'airflow',
    'email': ['nigud.sushant@gmail.com'],
    'provide_context': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'on_failure_callback': on_failure_callback,
}

dag = DAG('LG-dynamic-890123082',
        description='testing unit module example',
        schedule_interval='@once',
        start_date=datetime(2020, 12, 10),
        on_failure_callback=on_failure_callback,
        catchup=False,
        default_args=args)

def update_stopped_status(**kwargs):

    hook = AwsLambdaHook('arn:aws:lambda:ap-southeast-1:797237262327:function:ProjectCompletionStep-devthree',
                         region_name=region,
                         qualifier='$LATEST',
                         invocation_type='RequestResponse',
                         aws_conn_id='aws_default')

    payload = '{"workflow_execution_id" : "890123082"}'
    response_3 = hook.invoke_lambda(payload=payload)
    print(response_3)
    delete_s3_outputs()

start_worflow = DummyOperator(task_id='start', retries = 3, dag=dag)
end_workflow = PythonOperator(task_id='end', python_callable=update_stopped_status, dag=dag)

def create_dynamic_module(task_type, task_id, resource_type, states):
    _state_name = task_id.replace("_"," ")
    # print(task_type,task_id,resource_type)
    if task_type == "Task":
        if resource_type == 'arn:aws:states:::lambda:invoke':
            if task_id == 'Creating_Resources':
                # print('Creating_Resources')
                # dynamicTask = task_id
                dynamicTask = create_dynamic_operator('{}'.format(task_id), '{}'.format(task_id))
            elif task_id == 'Get_Resource_Creation_Status':
                # print('Get_Resource_Creation_Status in paralell')
                # dynamicTask = task_id
                dynamicTask = create_dynamic_operator('{}'.format(task_id), '{}'.format(task_id))
            elif task_id == 'Deleting_Resources':
                #  print('Deleting_Resources in parallel')
                #  dynamicTask = task_id
                dynamicTask = create_dynamic_operator('{}'.format(task_id), '{}'.format(task_id))
            else:
                # print('test in parallel')
                # dynamicTask = task_id
                dynamicTask = LambdaOperatorBuilder().getOperator(dag, task_id, states, context)
        elif resource_type == 'arn:aws:states:::sns:publish':
            dynamicTask = NotificationOperatorBuilder().getOperator(dag, task_id, states)
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
                dynamicTask = ECSOperatorBuilder().getOperator(dag, task_id, states, 890123082)
            elif ecs_type=='ecs-ec2':
                dynamicTask = ECSEC2ALLOperatorBuilder().getOperator(dag, task_id, states, 890123082)
        elif resource_type == 'arn:aws:states:::sagemaker:createTransformJob.sync':
            dynamicTask = SageMakerOperatorBuilder().getOperator(dag, task_id, states, context)
        elif resource_type == 'arn:aws:states:::elasticmapreduce:addStep.sync':
            dynamicTask = EmrOperatorBuilder().getOperator(dag, task_id, states)
        elif resource_type == 'arn:aws:states:::glue:startJobRun.sync':
            dynamicTask = GlueOperatorBuilder().getOperator(dag, task_id, states, context)
            # print('notify in parallel')
            # dynamicTask = task_id
    else:
        dynamicTask = create_dynamic_operator('{}'.format(task_id), '{}'.format(task_id))
        # dynamicTask = task_id
    return (dynamicTask)

def get_file_location(task_type, task_id, resource_type, states):
    _state_name = task_id.replace("_"," ")
    status_file_path="success"
    if task_type == "Task":
        if resource_type == 'arn:aws:states:::lambda:invoke':
            if task_id == 'Creating_Resources':
                pass
            elif task_id == 'Get_Resource_Creation_Status':
                pass
            elif task_id == 'Deleting_Resources':
                pass
            else:
                # For the Lambda
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
            if task_id == 'Creating_Resources':
                pass
            elif task_id == 'Get_Resource_Creation_Status':
                pass
            elif task_id == 'Deleting_Resources':
                pass
            else:
                # For the Lambda
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
        x=get_save_skip_status(task_type, task_id, resource_type, states)
        _dynamic_task = create_dynamic_module(task_type, task_id, resource_type, states)
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

create_dynamic_branch(updated_definition, start_worflow, end_workflow)