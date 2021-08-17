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

class ECSOperatorBuilder:
    
    def callableFunction(self,taskid,states,workflow_execution_id,**kwargs):
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

        networkConfiguration= states.get(taskname).get('Parameters').get('NetworkConfiguration')
        subnets=networkConfiguration.get('AwsvpcConfiguration').get('Subnets')
        securityGroups= networkConfiguration.get('AwsvpcConfiguration').get('SecurityGroups')

        overrides = states.get(taskname).get('Parameters').get('Overrides').get('ContainerOverrides')
        # print(overrides)
        # print(type(overrides))

        name = states.get(taskname).get('Parameters').get('Overrides').get('ContainerOverrides')[0].get('Name')
        # print('name is')
        # print(name)

        name1 = states.get(taskname).get('Parameters').get('Overrides').get('ContainerOverrides')[1].get('Name')
        # print('name1 is')
        # print(name1)

        overrides=states.get(taskname).get('Parameters').get('Overrides')
        Environment0=overrides.get('ContainerOverrides')[0].get('Environment')
        codeLocation = Environment0[0].get('Value')
        print(codeLocation)
        # iploc = []
        # for i in range(len(Environment0[1].get('Value'))):
        #     iploc.append(Environment0[1].get('Value')[0])
        #     print('iploc is:',iploc)

        # input_location  = " ".join(iploc)
        input_location =Environment0[1].get('Value')
        print('input_location',input_location)
        # input_file = Environment0[2].get('Value')
        # print(input_file)
        config_string=Environment0[2].get('Value')
        print('Module Configuration',config_string)

        Environment1=overrides.get('ContainerOverrides')[1].get('Environment')
        #print('env1')
        #print(Environment1)
        # output_file = Environment1[0].get('Value')
        # print(output_file)
        output_location = Environment1[0].get('Value')
        print(output_location)
        
        client = boto3.client('ecs',region_name=region)
        response = client.run_task(
            cluster = cluster,
            launchType= "FARGATE",
            networkConfiguration={
                'awsvpcConfiguration': {
                    'subnets': subnets,
                    #'subnets': ['subnet-0e824c46'],
                    'securityGroups': securityGroups,
                    #'securityGroups': ['sg-040ba6857f1980fe9'],
                    'assignPublicIp': 'ENABLED'
                }
            },
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
        print("Run Task response:", response)
        current_task=response.get('tasks')[0]['taskArn']
        print('taskArn is', current_task)
        tasksarns.append(current_task.rsplit('/',1)[1]+"_DeploymentECS")
        print(tasksarns)
        responsedb['tags']=tasksarns
        print(responsedb)
        table.put_item(Item=responsedb)
        
        #print('desc_task',desc_task)
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

    def getOperator(self, dag, task_id, states,workflow_execution_id):
        operator = PythonOperator(
	    task_id = task_id,
	    provide_context=True,
	    python_callable =ECSOperatorBuilder().callableFunction,
            op_kwargs={'taskid': task_id, 'states': states, 'workflow_execution_id':workflow_execution_id},
            dag = dag,
	)
        return operator
