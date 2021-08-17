import yaml
import json
import boto3
import uuid
import base64
import time
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook


def create_ecs_instance(region, create_ecs_instance_, encoded, cluster, workflow_execution_id, DYNMODB_TABLE_NAME):

    dynamodb = boto3.resource('dynamodb')
    dynamodb_table_name = DYNMODB_TABLE_NAME
    table = dynamodb.Table(dynamodb_table_name)
    workflow_execution_id_ = int(workflow_execution_id)
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
    
    hook = AwsLambdaHook(create_ecs_instance_,
                        region_name=region,
                        log_type='None',
                        qualifier='$LATEST',
                        invocation_type='RequestResponse',
                        config=None,
                        aws_conn_id='aws_default')

    payload = '{"stackName": "%s", "cft_template": "%s" }' % (stack_name, encoded)
    response_1 = hook.invoke_lambda(payload=payload)

    time.sleep(5)

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

    while True:
        client_ecs = boto3.client('ecs',region_name=region)
        response = client_ecs.describe_clusters(clusters=[cluster])
        instance_count = response['clusters'][0]['registeredContainerInstancesCount']
        if instance_count >= 1:
            print("New EC2 instance is attached to ECS cluster")
            break

    
    return stack_name


def delete_ecs_instance(region, stack_name, delete_ecs_instance_):

    stack_name = stack_name
    Function_name = delete_ecs_instance_
    hook = AwsLambdaHook(Function_name,
                         region_name=region,
                         qualifier='$LATEST',
                         invocation_type='RequestResponse',
                         aws_conn_id='aws_default')
    payload = '{"stackName": "%s"}' % (stack_name)
    response_2 = hook.invoke_lambda(payload=payload)
    return response_2

class ECSEC2OperatorBuilder:
    
    def callableFunction(self,taskid,states,workflow_execution_id,DYNMODB_TABLE_NAME,**kwargs):
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

        taskname=taskid.replace('_',' ')
        print(taskname)

        cluster=states.get(taskname).get('Parameters').get('Cluster')

        TaskDefinition = states.get(taskname).get('Parameters').get('TaskDefinition')
        overrides = states.get(taskname).get('Parameters').get('Overrides').get('ContainerOverrides')
        name = states.get(taskname).get('Parameters').get('Overrides').get('ContainerOverrides')[0].get('Name')
        name1 = states.get(taskname).get('Parameters').get('Overrides').get('ContainerOverrides')[1].get('Name')
        overrides=states.get(taskname).get('Parameters').get('Overrides')
        Environment0=overrides.get('ContainerOverrides')[0].get('Environment')
        codeLocation = Environment0[0].get('Value')
        #print(codeLocation)
        input_location =Environment0[1].get('Value')
        #print('input_location',input_location)
        config_string=Environment0[2].get('Value')
        print('Module Configuration',config_string)

        Environment1=overrides.get('ContainerOverrides')[1].get('Environment')

        output_location = Environment1[0].get('Value')
        print(output_location)

        tags=states.get(taskname).get('Parameters').get('tags')
        #print('tags are',tags)
        teamclusterkey=tags[1].get('key')
        #print(teamclusterkey)
        teamcluster=tags[1].get('value')
        autoscaling=tags[2].get('value')
        

        client = boto3.client('ecs',region_name=region)
        # Checking if any instance is registered with cluster
        for i in range(60):
            cluster_info = client.describe_clusters(
                clusters=[cluster]
            )

            if cluster_info['clusters'][0]['registeredContainerInstancesCount'] == 0:
                print(f"Attempt {i}: Instance did not register itself to cluster yet.")
            else:
                break

            time.sleep(5)

        TeamCluster = True
        if not teamcluster:
            task_id_def = taskid.replace("_", " ")
            #cluster_name = cluster
            encoded = states[task_id_def]["Parameters"]["Payload"]["cft_template"]
            create_ecs_instance_ = states[task_id_def]["Parameters"]["Payload"]["create_resources_function"]
            delete_ecs_instance_ = states[task_id_def]["Parameters"]["Payload"]["delete_resources_step"]
            stack_name = create_ecs_instance(region, create_ecs_instance_, encoded, cluster, workflow_execution_id, DYNMODB_TABLE_NAME)
            #raise ValueError('Fail this job')
            response = client.run_task(
                cluster = cluster,
                #launchType= "EC2",
                overrides={
                    'containerOverrides': [
                        {
                            'name': 'container1',
                            'environment': [
                                {
                                    'name': 'MODULE_LOC',
                                    'value': codeLocation
                                },
                                {
                                    'name': 'INPUT_LOC',
                                    'value': input_location
                                },
                                # {
                                #     'name': 'INPUT_FILE',
                                #     'value': input_file
                                # },
                                {
                                    'name': 'CONFIG_FILE',
                                    'value': config_string
                                },
                            ],
                        },
                        {
                            'name': 'container3',
                            'environment': [
                                # {
                                #     'name': 'OUTPUT_FILE',
                                #     'value': output_file
                                # },
                                {
                                    'name': 'OUTPUT_LOC',
                                    'value': output_location
                                },
                            ],
                        },
                    ],
                },
                taskDefinition=TaskDefinition,
				propagateTags= "TASK_DEFINITION"
            )
        else:
            if autoscaling:
                response = client.run_task(
                    cluster = cluster,
                    taskDefinition = TaskDefinition,
                    propagateTags= "TASK_DEFINITION",

                    placementConstraints= [
                            {
                                "type": "distinctInstance"
                            },
                        ],
                    overrides={
                        'containerOverrides': [
                            {
                                'name': 'container1',
                                'environment': [
                                    {
                                        'name': 'MODULE_LOC',
                                        'value': codeLocation
                                    },
                                    {
                                        'name': 'INPUT_LOC',
                                        'value': input_location
                                    },
                                    {
                                        'name': 'CONFIG_FILE',
                                        'value': config_string
                                    },
                                ],
                            },
                            {
                                'name': 'container3',
                                'environment': [
                                    {
                                        'name': 'OUTPUT_LOC',
                                        'value': output_location
                                    },
                                ],
                            },
                        ],
                    }
                )
            else:
                try:
                    singleinst=tags[3].get('value')
                    if singleinst:
                        placementConstraints=states.get(taskname).get('Parameters').get('PlacementConstraints')
                        print(placementConstraints)
                        placementConstraints_exp=placementConstraints[0].get('Expression')
                        print(placementConstraints_exp)
                        response = client.run_task(
                            cluster = cluster,
                            taskDefinition = TaskDefinition,
                            #launchType = "EC2",
                            propagateTags= "TASK_DEFINITION",
                            overrides={
                                'containerOverrides': [
                                    {
                                        'name': 'container1',
                                        'environment': [
                                            {
                                                'name': 'MODULE_LOC',
                                                'value': codeLocation
                                            },
                                            {
                                                'name': 'INPUT_LOC',
                                                'value': input_location
                                            },
                                            # {
                                            #     'name': 'INPUT_FILE',
                                            #     'value': input_file
                                            # },
                                            {
                                                'name': 'CONFIG_FILE',
                                                'value': config_string
                                            },
                                        ],
                                    },
                                    {
                                        'name': 'container3',
                                        'environment': [
                                            # {
                                            #     'name': 'OUTPUT_FILE',
                                            #     'value': output_file
                                            # },
                                            {
                                                'name': 'OUTPUT_LOC',
                                                'value': output_location
                                            },
                                        ],
                                    },
                                ],
                            },
                            placementConstraints=[
                                    {
                                        'type': 'memberOf',
                                        #'expression': 'attribute:ecs.instance-type =~ t2.micro',
                                        'expression': placementConstraints_exp,
                                        },
                                ],
                        )
                    else:
                        response = client.run_task(
                            cluster = cluster,
                            taskDefinition = TaskDefinition,
                            #launchType = "EC2",
                            propagateTags= "TASK_DEFINITION",
                            overrides={
                                'containerOverrides': [
                                    {
                                        'name': 'container1',
                                        'environment': [
                                            {
                                                'name': 'MODULE_LOC',
                                                'value': codeLocation
                                            },
                                            {
                                                'name': 'INPUT_LOC',
                                                'value': input_location
                                            },
                                            {
                                                'name': 'CONFIG_FILE',
                                                'value': config_string
                                            },
                                        ],
                                    },
                                    {
                                        'name': 'container3',
                                        'environment': [
                                            {
                                                'name': 'OUTPUT_LOC',
                                                'value': output_location
                                            },
                                        ],
                                    },
                                ],
                            }
                        )

                except:
                    response = client.run_task(
                        cluster = cluster,
                        taskDefinition = TaskDefinition,
                        #launchType = "EC2",
                        propagateTags= "TASK_DEFINITION",
                        overrides={
                            'containerOverrides': [
                                {
                                    'name': 'container1',
                                    'environment': [
                                        {
                                            'name': 'MODULE_LOC',
                                            'value': codeLocation
                                        },
                                        {
                                            'name': 'INPUT_LOC',
                                            'value': input_location
                                        },
                                        {
                                            'name': 'CONFIG_FILE',
                                            'value': config_string
                                        },
                                    ],
                                },
                                {
                                    'name': 'container3',
                                    'environment': [
                                        {
                                            'name': 'OUTPUT_LOC',
                                            'value': output_location
                                        },
                                    ],
                                },
                            ],
                        }
                    )

        print("Run Task response:", response)
        current_task=response.get('tasks')[0]['taskArn']
        print('taskArn is', current_task)
        tasksarns.append(current_task.rsplit('/',1)[1]+"_DeploymentECSEC2")
        print(tasksarns)
        responsedb['tags']=tasksarns
        print(responsedb)
        table.put_item(Item=responsedb)
        
        while True:
            time.sleep(2)
            desc_task = client.describe_tasks(
                cluster=cluster,
                tasks=[current_task],
                )   

            status = desc_task.get('tasks')[0]['lastStatus']
            if status in ['DEPROVISIONING', 'STOPPED']:
                print('ECS Task completed!')
                break
            else:
                print("Task Status: ", status)
        print("successfull")

        try:
            delete_ecs_instance(region, stack_name, delete_ecs_instance_)
        except:
            print("No cluster to delete")


    def getOperator(self, dag, task_id, states,workflow_execution_id, DYNMODB_TABLE_NAME):
        operator = PythonOperator(
	    task_id = task_id,
	    provide_context=True,
	    python_callable =ECSEC2OperatorBuilder().callableFunction,
            op_kwargs={'taskid': task_id, 'states': states,'workflow_execution_id':workflow_execution_id, "DYNMODB_TABLE_NAME": DYNMODB_TABLE_NAME},
            dag = dag,
	)
        return operator
