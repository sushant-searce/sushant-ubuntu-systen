"""
Takes in a dict and returns a dict with placeholders replaced from the context.
"""
import uuid
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key, Attr
import json
from stepfunctions.steps import Parallel, Pass
from stepfunctions.steps import Choice, ChoiceRule, Pass
from stepfunctions.steps import Chain
import constants
import yaml
import time
import re

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(constants.DYNMODB_TABLE)
s3 = boto3.resource('s3')
s3client = boto3.client('s3')
key_name = 'cfn_template.yaml'
region = s3client.meta.region_name
s3bucket_name = constants.SC_S3_BUCKET
sc_portfolio_id = constants.SC_PORTFOLIO_ID

def generate_temp_config(ModuleID, config_string):
    timestamp = str(uuid.uuid1())
    config_key = "public/modules/"+ModuleID+"/TempConfig/tempconfig-"+timestamp+".yaml"
    testconfigpath = "s3://"+s3bucket_name+"/"+config_key
    s3_upload = s3client.put_object(Bucket=s3bucket_name,Key=config_key, Body=config_string)
    return testconfigpath

def fetch_config_from_db(config_id, type):
    key = {
        "id": int(config_id),
        "type": type
    }
    response = table.get_item(Key=key)
    config_string: str = ""
    if type == constants.WORKFLOW_CONFIG_VERSION:
        config_string = response['Item'].get('workflowVersionConfig')
    else:
        config_string = response['Item'].get('moduleConfig')

    return yaml.full_load(config_string)

def fetch_module_config_from_db(module_id, workflow_version_id):
    key = {
        "id": int(workflow_version_id),
        "type": constants.WORKFLOW_CONFIG_VERSION
    }
    response = table.get_item(Key=key, ProjectionExpression='workflowVersionModules')['Item']['workflowVersionModules']
    config_string = next((item for item in response if item['id'] == module_id), None)['moduleConfig']
    platformModuleId = next((item for item in response if item['id'] == module_id), None)['platformModuleId']
    module_name = next((item for item in response if item['id'] == module_id), None)['moduleName']
    return {'config': yaml.full_load(config_string),
            'config_string': config_string,
            'platformModuleId': platformModuleId,
            'module_name': module_name}

# TODO: Need to iterate the Parallel Stack and Choice Stack and get the Modules referred there!
def fetch_deployment_config(workflow_version_id):
    deployment_config = []
    workflow_yaml = fetch_config_from_db(workflow_version_id, constants.WORKFLOW_CONFIG_VERSION)
    modules = workflow_yaml['modules']
    for module in modules.items():
        if "Choice" not in module[0]:
            temp_deployment_config = {}
            module_id = module[1].get("id")
            #config_location = module[1].get('ConfigLocation') # Only needed when yaml is in S3
            platform_id = fetch_module_config_from_db(module_id, workflow_version_id).get('platformModuleId')
            module_config = fetch_module_config_from_db(module_id, workflow_version_id).get('config')
            module_deployment_config = module_config.get('deployment') or module_config.get('Deployment')
            module_meta_data = module_config.get("metaData")

            temp_deployment_config['config'] = module_deployment_config
            temp_deployment_config['type'] = module_deployment_config['type']
            temp_deployment_config['name'] = module_meta_data['name']
            temp_deployment_config['description'] = module_meta_data['description']
            temp_deployment_config['moduleid'] = platform_id

            deployment_config.append(temp_deployment_config)
    return {"deployments":deployment_config}


def save_workflow_definition(config_id, definition, template, html):
    key = {
        "id": int(config_id),
        "type": constants.WORKFLOW_CONFIG_VERSION
    }
    response = table.get_item(Key=key)['Item']
    response['workflowVersionASLDef'] = str(definition)
    response['workflowVersionCFNTemplate'] = template
    response['workflowVersionGraphSVG'] = html
    response['creation_status'] = "CREATED"
    table.put_item(Item=response)
    return True


def publish(template_value, workflow_version_id):
    key = {
        "id": int(workflow_version_id),
        "type": constants.WORKFLOW_CONFIG_VERSION
    }
    workflow_version_properties = table.get_item(Key=key)['Item']

    s3.Object(s3bucket_name, key_name).put(Body=template_value)
    s3_url=f'https://{s3bucket_name}.s3.{region}.amazonaws.com/{key_name}'
    client = boto3.client('servicecatalog')
    create_product_response = client.create_product(
        Name=workflow_version_properties.get('workflowVersionDescription')+str(datetime.now()),
        Owner=workflow_version_properties.get('userRole'),
        Description=workflow_version_properties.get('workflowVersionDescription'),
        SupportEmail='test@support.com',
        ProductType='CLOUD_FORMATION_TEMPLATE',
        ProvisioningArtifactParameters={
            'Name': 'InitialCreation',
            'Description': 'InitialCreation',
            'Info': {
                'LoadTemplateFromURL': s3_url
            },
            'Type': 'CLOUD_FORMATION_TEMPLATE'
        },
        IdempotencyToken=str(uuid.uuid4())
    )
    response = client.associate_product_with_portfolio(
        ProductId=create_product_response['ProductViewDetail']['ProductViewSummary']['ProductId'],
        PortfolioId=sc_portfolio_id
    )
    client.associate_service_action_with_provisioning_artifact(
        ProductId=create_product_response['ProductViewDetail']['ProductViewSummary']['ProductId'],
        ProvisioningArtifactId=create_product_response['ProvisioningArtifactDetail']['Id'],
        ServiceActionId='act-yy3sbkqpyjmqy'
    )
    return response


def resolvePipelineVariables(input, context):
    expr, append_string = input.split("}")
    expr = expr.strip("${").strip("}")
    return eval(expr) + append_string


def resolveStepVariables(input, context):
    # expr = input.strip("${").strip("}")
    return "$"+input.split('output()')[1].strip("}""")


def evaluateChoiceVariables(input, context):
    # expr, append_string = input.split("}")
    # expr = expr.strip("${").strip("}")
    # attribute = append_string.split('[', 1)[1].split(']')[0].strip("'")
    return resolveStepVariables(input,context)+['StatusCode']


def resolvePlaceHolderValues(value, context):
    if value.find("pipeline_execution") > -1:
        return resolvePipelineVariables(value, context)
    elif value.find("step_execution") > -1:
        return resolveStepVariables(value, context)
    else:
        return value


def resolvePlaceHoldersDict(parameters, context):
    out_parameters = {}
    for k, v in parameters.items():
        try:
            if type(v) == str and v.find("pipeline_execution") > -1:
                out_parameters[k] = resolvePipelineVariables(v, context)

            elif type(v) == str and v.find("step_execution") > -1:
                out_parameters[k+".$"] = resolveStepVariables(v, context)
            else:
                out_parameters[k] = v
        except:
            print('Exception while Replacing Parameters..')
            raise Exception("One or more of the input params in the yaml definition was wrong")

    return out_parameters


def check_and_update_create_status(workflow_version_id, status):
    key = {
        "id": int(workflow_version_id),
        "type": constants.WORKFLOW_CONFIG_VERSION
    }
    response = table.get_item(Key=key)['Item']
    if (response.get('creation_status') == 'RUNNING'):
        response['creation_status'] = 'RUNNING'
        table.put_item(Item=response)
        return True
    else:
        return False


def fetch_workflow_svg(workflow_id):
    key = {
        "id": int(workflow_id),
        "type": constants.WORKFLOW_CONFIG_VERSION
    }
    response = table.get_item(Key=key)['Item']
    return response.get('workflowVersionGraphSVG')


def ordered_config(config):
    ordered_step = []
    first_module = config.get('start')
    for k, v in config.items():
        #if (v.get('Start') == 'True'):
        if k == first_module:
            ordered_step.insert(0, (k, v))

    lookout = True
    next = ordered_step[0][1].get('next')
    if next:
        while lookout:
            next_step = config.get(next)
            if (next_step.get('end') == 'True'):
                ordered_step.append((next, next_step))
                lookout = False
                break
            ordered_step.append((next, next_step))
            next = next_step.get('next')

    response = {}
    for step in ordered_step:
        response[step[0]] = step[1]

    return response

class ChainableChoice(Choice):
    def next(self, next_step):
        self.default_choice(next_step)
        return next_step


def airflow_parallel(branches,config):
    res=[]
    intersections=[]
    print("branches:",branches)
    for i in branches:
        branch = []
        step_def = config.get(i)
        branch.append(i)
        next=step_def.get("next")
        while next:
            step_def =config.get(next)
            branch.append(next)
            next = step_def.get("next")    
        res.append(branch)
        print("branch:",branch) 
    length=len(res)
    for i in range(0,length-1):
        intersect=sorted(set(res[i]) & set(res[i+1]), key = res[i].index)
        if intersect:
            intersections.append(intersect[0])
    intersections=set(intersections)
    if intersections:
        print("intersections:",intersections)
        next = list(intersections)[0]        
        print("next:",next)    
    else:
        next = None
    return next

def airflow_choice(condition_list,config):
    res=[]
    intersections=[]
    print("condition_list",condition_list)
    for condition in condition_list:
        item = condition.get('Next')
        print("item",item)
        branch = []
        step_def = config.get(item)
        branch.append(item)
        next=step_def.get("Next")
        while next:
            step_def =config.get(next)
            branch.append(next)
            next = step_def.get("Next")    
        res.append(branch) 
    length=len(res)
    for i in range(0,length-1):
        intersect=sorted(set(res[i]) & set(res[i+1]), key = res[i].index)
        if intersect:
            intersections.append(intersect[0])
    intersections=set(intersections)
    if intersections:
        print("intersections:",intersections)
        next = list(intersections)[0]       
        print("next:",next)     
    else:
        next = None
    return next


def create_dynamic_chain(state_name,config,context,intersect):
    state = config.get(state_name)
    print("dynamic_chain state_name:",state_name)
    
    next = state.get('next')
    print("dynamic_chain next:",next)
    print("dynamic_chain intersect:",intersect)
    if state.get('type')=='parallel':
        branches = state.get('branches')
        parallel_step = Parallel(state_id=state_name)
        intersect = airflow_parallel(branches,config)
        for branch in branches:
            branch_chain = create_dynamic_chain(branch,config,context,intersect)
            parallel_step.add_branch(branch_chain)
        if intersect:
            chain_list=list(create_dynamic_chain(intersect,config,context,None))
            module = [parallel_step]
            response = module + chain_list 
            chain = Chain(response)
            return chain
        else:
            module = [parallel_step]
            chain = Chain(module)
            return chain
    if state.get('type')=='choice':
        condition_list=state.get('conditions')
        choice_step = ChainableChoice( state_id=state_name)
        intersect = airflow_choice(condition_list,config)
        for condition in condition_list:
            variable = condition.get('Variable')
            expression = condition.get('Expression')
            expressionValue = condition.get('ExpressionValue')
            next = condition.get('Next')
            branch_chain = create_dynamic_chain(next,config,context,intersect)
            if next:
                next_implementation = context['step_execution'][next]
                choice_rule = getattr(ChoiceRule, expression)(variable=variable, value=expressionValue)
                choice_step.add_choice(choice_rule, next_step = next_implementation)        
        if intersect:
            chain_list=list(create_dynamic_chain(intersect,config,context,None))
            module = [choice_step]
            response = module + chain_list 
            chain = Chain(response)
            return chain
        else:
            module = [choice_step]
            chain = Chain(module)
            return chain
    elif next == intersect:
        module= [context['step_execution'][state_name]]
        chain = Chain(module)
        return chain    
    else:        
        if next:
            if intersect:
                chain_list=list(create_dynamic_chain(next,config,context,intersect))    
            else:
                chain_list=list(create_dynamic_chain(next,config,context,None))
            module = [context['step_execution'][state_name]]
            response = module + chain_list 
            chain = Chain(response)
            return chain
        else:
            module= [context['step_execution'][state_name]]
            chain = Chain(module)
            return chain


def airflow_ordered_config(config,context):
    first_module = config.get('start')
    ans=list(Chain([context['step_execution'][first_module]]))
    next = config.get(first_module).get('next')
    if next == None:
        return ans
    else:
        chain_list = list(create_dynamic_chain(next,config,context,None))
        response = ans + chain_list
        return response


def get_project_output(workflow_version_id):

    config_yaml = fetch_config_from_db(workflow_version_id, constants.WORKFLOW_CONFIG_VERSION)
    config_unordered = config_yaml.get('steps')
    for k, v in config_unordered.items():
        if(k != "start"):
            if(v.get('end') == 'True'):
                module_id = config_yaml['modules'][k]['id']
                input_yaml = fetch_module_config_from_db(module_id,workflow_version_id).get('config')
                last_module_output_loc = input_yaml['output']['dest'][0]['filePath']
    return last_module_output_loc


def update_module_config(workflow_version_id,workflow_execution_id):
    key = {
        "id": int(workflow_version_id),
        "type": constants.WORKFLOW_CONFIG_VERSION
    }
    response = table.get_item(Key=key, ProjectionExpression='workflowVersionModules')['Item']['workflowVersionModules']
    config_yaml = fetch_config_from_db(workflow_version_id, constants.WORKFLOW_CONFIG_VERSION)
    wfconfig = config_yaml.get('steps')
    config = ordered_config(config_yaml.get('steps'))
    #tempnextinputname = ""
    tempnextinputname = {}
    #tempnextinputpath = ""
    tempnextinputpath = {}
    first_module = wfconfig.get('start')
    last_module_output_loc = ""
    updated = False
    step_count = 1
    for k, v in config.items():
        #if (v.get('Start') == 'True'):
        if (k == first_module) and (v.get('end') == 'True'):
            module_id = config_yaml['modules'][k]['id']
            input_yaml = fetch_module_config_from_db(module_id,workflow_version_id).get('config')
            last_module_output_loc = input_yaml['output']['dest'][0]['filePath'] #To update project where to find output file
            break;
        elif k == first_module:
            updated = True
            module_id = config_yaml['modules'][k]['id']
            input_yaml = fetch_module_config_from_db(module_id,workflow_version_id).get('config')
            input_yaml['metaData']['description'] = "["+str(step_count)+"]- "+input_yaml['metaData']['description'].split("]- ")[-1]
            step_count+=1
            for i in range(len(input_yaml['output']['dest'])):
                outputFileName_t = k+"_"+input_yaml['output']['dest'][i]['name']
                outputFileName = outputFileName_t.replace(" ","_")
                outputFilePath = "s3://"+s3bucket_name+"/public/Workflow/ExecutionFiles/"+str(workflow_version_id)+"/"+str(workflow_execution_id)+"/"+outputFileName
                tempnextinputname[i] = outputFileName
                tempnextinputpath[i] = outputFilePath
                input_yaml['output']['dest'][i]['name'] = outputFileName
                input_yaml['output']['dest'][i]['filePath'] = outputFilePath
            next((item for item in response if item['id'] == module_id), None)['moduleConfig'] = yaml.dump(input_yaml)

        elif v.get('end') == 'True':
            updated = True
            inputFileName = {}
            inputFilePath = {}
            module_id = config_yaml['modules'][k]['id']
            input_yaml = fetch_module_config_from_db(module_id,workflow_version_id).get('config')
            input_yaml['metaData']['description'] = "["+str(step_count)+"]- "+input_yaml['metaData']['description'].split("]- ")[-1]
            step_count+=1
            print(input_yaml)
            last_module_output_loc = input_yaml['output']['dest'][0]['filePath'] #To update project where to find output file
            for i in range(len(input_yaml['input']['source'])):
                inputFileName[i] = tempnextinputname[i]
                inputFilePath[i] = tempnextinputpath[i]
                input_yaml['input']['source'][i]['name'] = tempnextinputname[i]
                input_yaml['input']['source'][i]['filePath'] = tempnextinputpath[i]
            next((item for item in response if item['id'] == module_id), None)['moduleConfig'] = yaml.dump(input_yaml)
        else:
            updated = True
            inputFileName = {}
            inputFilePath = {}
            module_id = config_yaml['modules'][k]['id']
            input_yaml = fetch_module_config_from_db(module_id,workflow_version_id).get('config')
            input_yaml['metaData']['description'] = "["+str(step_count)+"]- "+input_yaml['metaData']['description'].split("]- ")[-1]
            step_count+=1
            for i in range(len(input_yaml['input']['source'])):
                inputFileName[i] = tempnextinputname[i]
                inputFilePath[i] = tempnextinputpath[i]
                input_yaml['input']['source'][i]['name'] = inputFileName[i]
                input_yaml['input']['source'][i]['filePath'] = inputFilePath[i]
            for i in range(len(input_yaml['output']['dest'])):
                outputFileName_t = k+"_"+input_yaml['output']['dest'][i]['name']
                outputFileName = outputFileName_t.replace(" ","_")
                outputFilePath = "s3://"+s3bucket_name+"/public/Workflow/ExecutionFiles/"+str(workflow_version_id)+"/"+str(workflow_execution_id)+"/"+outputFileName
                tempnextinputname[i] = outputFileName
                tempnextinputpath[i] = outputFilePath
                input_yaml['output']['dest'][i]['name'] = outputFileName
                input_yaml['output']['dest'][i]['filePath'] = outputFilePath
            next((item for item in response if item['id'] == module_id), None)['moduleConfig'] = yaml.dump(input_yaml)
    if(updated):
        table.update_item(
            Key= key,
            UpdateExpression="set workflowVersionModules=:r",
            ExpressionAttributeValues={
                ':r': response
            },
            ReturnValues="UPDATED_NEW"
        )

    return last_module_output_loc

def get_project_output(workflow_version_id):
    config_yaml = fetch_config_from_db(workflow_version_id, constants.WORKFLOW_CONFIG_VERSION)
    config_unordered = config_yaml.get('steps')
    for k, v in config_unordered.items():
        if(k != "start"):
            if(v.get('end') == 'True'):
                module_id = config_yaml['modules'][k]['id']
                input_yaml = fetch_module_config_from_db(module_id,workflow_version_id).get('config')
                last_module_output_loc = input_yaml['output']['dest'][0]['filePath']
    return last_module_output_loc

def update_module_config_new_version(workflow_version_id,workflow_execution_id):
    key = {
        "id": int(workflow_version_id),
        "type": constants.WORKFLOW_CONFIG_VERSION
    }
    response = table.get_item(Key=key, ProjectionExpression='workflowVersionModules')['Item']['workflowVersionModules']
    for item in response:
        if item.get('moduleConfig'):
            item['moduleConfig'] = re.sub("/Workflow/ExecutionFiles/[0-9]+/[0-9]+/","/Workflow/ExecutionFiles/"+str(workflow_version_id)+"/"+str(workflow_execution_id)+"/",item['moduleConfig'])
    table.update_item(
        Key= key,
        UpdateExpression="set workflowVersionModules=:r",
        ExpressionAttributeValues={
            ':r': response
        },
        ReturnValues="UPDATED_NEW"
    )

def fetch_config_from_s3(s3_location):
    bucket_name = s3_location.split('/')[2]
    key = '/'.join(s3_location.split('/')[3:])

    s3resource = boto3.resource('s3')
    object = s3resource.Object(bucket_name, key)
    config_string = object.get()['Body'].read().decode('utf-8')

    return yaml.full_load(config_string)

def check_choice_parallel(workflow_version_id):
    config_yaml = fetch_config_from_db(workflow_version_id, constants.WORKFLOW_CONFIG_VERSION)
    steps = config_yaml.get('steps')
    for step_name in steps:
        if step_name.startswith('parallel') or step_name.startswith('Choice'):
            return "Sorry, we can not handle Parallel or Choice states as of now. Please remove them and try again."
    return False

def check_and_update_execution_status(workflow_version_id, context):
    # Get the Execution Status
    items = table.query(
        KeyConditionExpression=Key('type').eq('workflowExecution'),
        FilterExpression=Attr('parent').eq((workflow_version_id)) & Attr('execution_status').eq('RUNNING')
    )['Items']

    # Allow only one running execution at a time.
    if len(items) < 1:
        key = {
            "id": int(workflow_version_id),
            "type": constants.WORKFLOW_CONFIG_VERSION
        }
        response = table.get_item(Key=key)['Item']
        workflow_execution_version_id = str(round(time.time()*1000))[4:]
        # if response["ModuleCategory"] == "module-test":
        #     item_type = "testModuleExecution"
        # else:
        #     item_type = constants.WORKFLOW_EXECUTION_VERSION
        table.put_item(Item={
            "id": int(workflow_execution_version_id),
            "type": constants.WORKFLOW_EXECUTION_VERSION,
            "workflowVersionName": response["workflowVersionName"],
            "userName": context['tags']['user'],
            "version": response["version"],
            "Team": context['tags']['team'],
            "parent": int(workflow_version_id),
            "createdAt": datetime.now().strftime("%d/%m/%Y %H:%M:%S.%f"),
            "execution_status": "INITIALIZING",
            "ModuleCategory": response['ModuleCategory'],
            "workflowId": response['parent'],
            "workflowVersionDescription": context['executionDescription'],
            "scheduleId": context['scheduleId']
        })
        return workflow_execution_version_id
    else:
        return False

def update_output_loc_in_db(workflow_version_id, output_loc, workflow_execution_id):
    key_ex = {
        "id": int(workflow_execution_id),
        "type": constants.WORKFLOW_EXECUTION_VERSION
    }
    executionresponse = table.get_item(Key=key_ex)['Item']
    executionresponse['outputLocation'] = output_loc
    table.put_item(Item=executionresponse)
    key_wv = {
        "id": int(workflow_version_id),
        "type": constants.WORKFLOW_CONFIG_VERSION
    }
    response_wv = table.get_item(Key=key_wv)['Item']
    parent_w = response_wv['parent']
    response_wv['outputLocation'] = output_loc
    table.put_item(Item=response_wv)

    if parent_w:
        key_w = {
            "id": int(parent_w),
            "type": "workflow"
        }
        response_w = table.get_item(Key=key_w)['Item']
        response_w['outputLocation'] = output_loc
        table.put_item(Item=response_w)

    return True

def save_execution_results_in_db(workflow_execution_id, html, execution_arn,template):
    print(execution_arn)
    key = {
        "id": int(workflow_execution_id),
        "type": constants.WORKFLOW_EXECUTION_VERSION
    }
    response = table.get_item(Key=key)['Item']
    parent_wv = response['parent']
    response['executionVersionGraphSVG'] = html
    response['execution_arn'] = execution_arn
    response['workflowVersionCFNTemplate'] = template
    response['execution_status'] = 'RUNNING'
    table.put_item(Item=response)

    key_wv = {
        "id": int(parent_wv),
        "type": constants.WORKFLOW_CONFIG_VERSION
    }
    response_wv = table.get_item(Key=key_wv)['Item']
    parent_w = response_wv['parent']
    response_wv['ModuleStatus'] = 0
    table.put_item(Item=response_wv)

    if parent_w:
        key_w = {
            "id": int(parent_w),
            "type": "workflow"
        }
        response_w = table.get_item(Key=key_w)['Item']
        response_w['ModuleStatus'] = 0
        table.put_item(Item=response_w)
    return True


def get_execution_arn(workflow_version_id, workflow_execution_id):
    key = {
        "id": int(workflow_execution_id),
        "type": constants.WORKFLOW_EXECUTION_VERSION
    }
    response = table.get_item(Key=key)['Item']
    return response.get('execution_arn')

def get_container_uri(module_id):
    key = {
        "id": int(module_id),
        "type": 'customModule'
    }
    response = table.get_item(Key=key)['Item']
    container_uri = response.get('DockerECRURI') + ':'+ response.get('ContainerName') #.split(':')[0][:-7]
    return container_uri

def get_artifact_location(module_id):
    key = {
        "id": int(module_id),
        "type": 'customModule'
    }
    response = table.get_item(Key=key)['Item']
    artifact_location = response.get('ModuleLocation')
    return artifact_location

'''
This method is responsible for creating a cluster and starting an EC2 in it for module testing with direct string
'''
def start_resource(context):
    try:
        ecs_client = boto3.client('ecs')
        cluster_name = 'testModuleCluster-'+ str(round(time.time()*1000))[3:]
        cluster = ecs_client.create_cluster(
            clusterName= cluster_name
        )

        ssmclient = boto3.client('ssm')
        json_string = ssmclient.get_parameter(Name='/aws/service/ecs/optimized-ami/amazon-linux-2/recommended')['Parameter']['Value']
        AMI = json.loads(json_string)['image_id']

        ec2_resource = boto3.resource('ec2')
        instance = ec2_resource.create_instances(
            ImageId= AMI,
            MinCount=1,
            MaxCount=1,
            InstanceType='t2.micro',
            IamInstanceProfile={
                'Arn': 'arn:aws:iam::925881846319:instance-profile/ecsInstanceRole'
            },
            SecurityGroupIds=['sg-050da7d38ee2884dc'],
            SubnetId = 'subnet-0f3429750577fffdf',
            UserData='#!/bin/bash\necho ECS_CLUSTER='+ cluster_name +' >> /etc/ecs/ecs.config',
            TagSpecifications=[
                {
                    'ResourceType': 'instance',
                    'Tags': [
                        {
                            'Key': 'Name',
                            'Value': 'Test Module Instance'
                        },
                        {
                            'Key': 'user',
                            'Value': context['tags']['user']
                        },
                        {
                            'Key': 'team',
                            'Value': context['tags']['team']
                        },
                        {
                            'Key': 'department',
                            'Value': context['tags']['department']
                        },
                        {
                            'Key': 'executionId',
                            'Value': context['tags']['executionId']
                        }
                    ]
                },
            ],
        )

        # time.sleep(25) # Time given for instance to boot and register with cluster
        instance_id = instance[0].id
        cloudWatch = boto3.client('cloudwatch')
        cloudWatch.put_metric_alarm(
            AlarmName          = f'CPU_ALARM_{instance_id}',
            AlarmDescription   = 'Alarm when server CPU does not exceed 10% for 15 minutes',
            AlarmActions       = [f'arn:aws:automate:{region}:ec2:terminate'],
            MetricName         = 'CPUUtilization',
            Namespace          = 'AWS/EC2' ,
            Statistic          = 'Average',
            Dimensions         = [{'Name': 'InstanceId', 'Value': instance_id}],
            Period             = 300,
            EvaluationPeriods  = 6,
            Threshold          = 10,
            ComparisonOperator = 'LessThanOrEqualToThreshold',
            TreatMissingData   = 'notBreaching',
            Tags = [
                {
                    'Key': 'user',
                    'Value': context['tags']['user']
                },
                {
                    'Key': 'team',
                    'Value': context['tags']['team']
                },
                {
                    'Key': 'department',
                    'Value': context['tags']['department']
                },
                {
                    'Key': 'executionId',
                    'Value': context['tags']['executionId']
                }
            ]
        )

        # time.sleep(25) # Time given for instance to boot and register with cluster
        # ec2_client = boto3.client('ec2')
        # waiter = ec2_client.get_waiter('instance_running')
        # waiter.wait(InstanceIds=[instance[0].id])
    except Exception as e:
        return {"status":"error", "message": 'Error while creating resources'}

    return {"status":"accepted", "message": cluster_name}

'''
This method is responsible for terminating a cluster and stoping an EC2 in it for module testing with direct string
'''
def stop_resource(clusterName):
    try:
        ecs_client = boto3.client('ecs')
        container_arns = ecs_client.list_container_instances(cluster=clusterName)['containerInstanceArns']
        if len(container_arns)==0:
            return {"status":"error", "message": 'Instance termination is not possible right after Deployment. Please try after some time.'}

        deregister_instance = ecs_client.deregister_container_instance(
            cluster= clusterName,
            containerInstance= container_arns[0],
            force=True
        )

        delete_cluster = ecs_client.delete_cluster(
            cluster= clusterName
        )

        instance_id = deregister_instance['containerInstance']['ec2InstanceId']

        alarm_name = f'CPU_ALARM_{instance_id}'
        cloudWatch = boto3.client('cloudwatch')
        cloudWatch.delete_alarms(
            AlarmNames=[alarm_name]
        )

        ec2 = boto3.client('ec2')
        terminate_ec2 = ec2.terminate_instances(
            InstanceIds=[instance_id]
        )
    except Exception as e:
        return {"status":"error", "message": 'Error while terminating resources'}

    return {"status":"accepted", "message": 'Resource terminated successfully'}

def count_cluster_instances(clusterName):
    ecs_client = boto3.client('ecs')
    cluster_details = ecs_client.describe_clusters(
        clusters= [clusterName]
    )
    return cluster_details['clusters'][0]['registeredContainerInstancesCount']

def update_ecs_task(context, container_uri, launch_type, cpu=256, memory=512):
    print(context['tags'])
    print('launch type in utils',launch_type)
    task_name = constants.LPP_FARGATE_TASK_DEF

    client = boto3.client('ecs')

    if launch_type == 'FARGATE':
        network_mode = 'awsvpc'
    elif launch_type == 'EC2':
        network_mode = 'bridge'

    response = client.register_task_definition(
        family=task_name,
        taskRoleArn=constants.TASK_EXECUTION_ROLE,
        executionRoleArn=constants.TASK_EXECUTION_ROLE,
        requiresCompatibilities=[launch_type],
        cpu= str(cpu),
        memory= str(memory),
        networkMode=network_mode,
        containerDefinitions=[
            {
                'name':'container1',
                'image':constants.STARTER_STOPPER,
                'essential':False,
                'portMappings': [
                    {
                        'containerPort':80,
                        # 'hostPort':80,
                        'protocol':'tcp'
                    }
                ],
                'entryPoint':[
                    "/tmp/home/start.sh"
                ],
                'mountPoints':[
                    {
                        'sourceVolume':'lpp',
                        'containerPath':'/tmp/home'
                    }
                ],
                'logConfiguration':{
                    'logDriver':'awslogs',
                    'options':{
                        "awslogs-group": constants.LOG_GROUP,
                        "awslogs-region": region,
                        "awslogs-stream-prefix": "ecs"
                    }
                }
            },
            {
                'name':'container2',
                'image':container_uri,
                'essential':False,
                'portMappings': [
                    {
                        'containerPort':81,
                        # 'hostPort':81,
                        'protocol':'tcp'
                    }
                ],
                'entryPoint':[
                    "python"
                ],
                'command':[
                    "yaml_parser.py",
                    "--yaml",
                    "tempconfig.yaml"
                ],
                'dependsOn':[
                    {
                        'condition':"COMPLETE",
                        'containerName':"container1"
                    }
                ],
                'workingDirectory': "/tmp/home",
                'mountPoints':[
                    {
                        'sourceVolume':'lpp',
                        'containerPath':'/tmp/home'
                    }
                ],
                'logConfiguration':{
                    'logDriver':'awslogs',
                    'options':{
                        "awslogs-group": constants.LOG_GROUP,
                        "awslogs-region": region,
                        "awslogs-stream-prefix": "ecs"
                    }
                }
            },
            {
                'name':'container3',
                'image':constants.STARTER_STOPPER,
                'essential':True,
                'portMappings': [
                    {
                        'containerPort':82,
                        # 'hostPort':82,
                        'protocol':'tcp'
                    }
                ],
                'entryPoint':[
                    "/tmp/home/end.sh"
                ],
                'dependsOn':[
                    {
                        'condition':"COMPLETE",
                        'containerName':"container2"
                    }
                ],
                'mountPoints':[
                    {
                        'sourceVolume':'lpp',
                        'containerPath':'/tmp/home'
                    }
                ],
                'logConfiguration':{
                    'logDriver':'awslogs',
                    'options':{
                        "awslogs-group": constants.LOG_GROUP,
                        "awslogs-region": region,
                        "awslogs-stream-prefix": "ecs"
                    }
                }
            }
        ],
        volumes= [
            {
                "name": "lpp",
                "host": {}
            }
        ],
        tags=[
            {
                'key': 'user',
                'value': context['tags']['user']
            },
            {
                'key': 'team',
                'value': context['tags']['team']
            },
            {
                'key': 'department',
                'value': context['tags']['department']
            },
            {
                'key': 'executionId',
                'value': context['tags']['executionId']
            }
        ]
    )

    return response['taskDefinition']['taskDefinitionArn']


