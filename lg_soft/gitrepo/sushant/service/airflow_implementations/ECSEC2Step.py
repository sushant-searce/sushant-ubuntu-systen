import json

from stepfunctions.steps import EcsRunTaskStep

from stepfunctions.steps import *
import utils
import constants
import base64
import os,sys,inspect
from Assemble import build_ecs_instance
from troposphere import Parameter, Ref, Template, GetAtt, Join, Output

def fetch_config_from_db(config_id, type):
    return utils.fetch_config_from_db(config_id, type)

def get_largest_instance_type(workflow_version_id):
    deployment_yaml=utils.fetch_deployment_config(workflow_version_id)
    deploy_list=deployment_yaml['deployments']
    inst_list=[]
    for i in deploy_list:
        if i['type']=='ecs-ec2':
            inst_type=i['config']['config']['InstanceType']
            inst_list.append(inst_type)
        else:
            pass
    avail_inst=constants.ECS_INSTANCE_LIST
    available_instance_list = avail_inst.strip('][').split(',')
    inst_list.sort(key = lambda i: available_instance_list.index(i))
    inst=str(inst_list[0])
    return inst
    
class ECSEC2StepBuilder:
    def getStep(self, context, module_config, workflow_version_id):
        module_id = module_config.get('platformModuleId')
        definition = module_config.get('config')
        config_string = module_config.get('config_string')
        module_name = module_config.get('module_name')
        workflow_module_id = module_config.get('module_id')
        
        t = Template()
        t.set_version("2010-09-09")
        _deployment_ = {}
        t.add_parameter(Parameter("RunId",Type="String"))
        deployment= definition['deployment']
        _deployment_["config"] = deployment
        _deployment_["type"] = "function"
        _deployment_["name"] = definition["metaData"]["name"]
        _deployment_["description"] = definition["metaData"]["description"]
        _deployment_["moduleid"] = module_id        
        cft = build_ecs_instance(_deployment_,context,t)
        
        if cft.to_dict()['Resources']:
            cft_definition = cft.to_yaml()
        
        encoded = base64.b64encode(cft_definition.encode('ascii')).decode('ascii')
        resource = {}
        resource["create_resources_function"]=constants.CREATE_RESOURCE
        resource["cft_template"]=encoded
        resource["delete_resources_step"]=constants.DELETE_RESOURCE
        
        config_yaml = fetch_config_from_db(workflow_version_id, constants.WORKFLOW_CONFIG_VERSION)
        container_uri = utils.get_container_uri(module_id)
        cpu = definition.get("deployment").get('config').get('CPU')
        memory = definition.get("deployment").get('config').get('Memory')
        launch_type=definition.get("deployment").get('config').get('launch_type')
        print(launch_type)
        task_arn = utils.update_ecs_task(context,container_uri, launch_type, cpu, memory)
        metadata = definition.get('metaData')
        state_id = module_name # metadata['description']

        iploc = []
        for i in range(len(definition.get('input').get('source'))):
            iploc.append(utils.resolvePlaceHolderValues(definition.get('input').get('source')[i].get('filePath'),context))
        if definition.get('input').get('dictionary'):
            for i in range(len(definition.get('input').get('dictionary'))):
                iploc.append(utils.resolvePlaceHolderValues(definition.get('input').get('dictionary')[i].get('filePath'),context))
        input_location  = " ".join(iploc)
        # output_file     = utils.resolvePlaceHolderValues(definition.get("output").get('dest')[0].get('name'),context)
        oploc = []
        for i in range(len(definition.get('output').get('dest'))):
            oploc.append(utils.resolvePlaceHolderValues(definition.get('output').get('dest')[i].get('filePath'),context))

        output_location  = " ".join(oploc)
        codeLocation    = utils.get_artifact_location(module_id)
        extra = {}
        modules = config_yaml.get('modules')
        for module in modules:
            if modules[module].get('id') == workflow_module_id:
                name = module
                break
        steps = config_yaml.get('steps')    
        extra['saveOutput'] = steps[name].get('saveOutput')
        extra['skipModule'] = steps[name].get('skipModule')

        team_name=context['tags']['team']
        cluster_name=team_name.replace(" ", "-")+'-cluster'
        print(cluster_name)


        TeamCluster = True
        Autoscaling = False

        if not TeamCluster:
            step = EcsRunTaskStep(
                state_id=state_id,
                parameters={
                    "Payload":resource,
                    "Cluster": constants.ECS_EC2_CLUSTER,
                    "TaskDefinition": task_arn,
                    "PropagateTags": "TASK_DEFINITION",
                    "PlacementConstraints": [{"Type": "distinctInstance"}],
                    "Overrides": {
                        "ContainerOverrides": [
                            {
                                "Name": 'container1',
                                "Environment": [
                                    {"Name": 'MODULE_LOC',"Value": codeLocation},
                                    {"Name": 'INPUT_LOC',"Value": input_location},
                                    {"Name": 'CONFIG_FILE',"Value": config_string}
                                ]
                            },
                            {
                                "Name": 'container3',
                                "Environment": [
                                    {
                                        "Name": 'OUTPUT_LOC',
                                        "Value": output_location
                                    }
                                ]
                            }
                        ]
                    },
                    "tags": [{"key":"extra","value":extra},
                            {"key":"teamcluster","value":TeamCluster},
                            {"key":"autoscaling","value":Autoscaling},
                            ]
                }
                )
        else:
            if Autoscaling:
                step = EcsRunTaskStep(
                    state_id=state_id,
                    parameters={
                        "Cluster": cluster_name, 
                        "TaskDefinition": task_arn,
                        "PropagateTags": "TASK_DEFINITION",
                        "PlacementConstraints": [{"Type": "distinctInstance"}],
                        "Overrides": {
                            "ContainerOverrides": [
                                {
                                        "Name": 'container1',
                                        "Environment": [
                                            {"Name": 'MODULE_LOC',"Value": codeLocation},
                                            {"Name": 'INPUT_LOC',"Value": input_location},
                                            {"Name": 'CONFIG_FILE',"Value": config_string}
                                        ]
                                    },
                                {
                                    "Name": 'container3',
                                    "Environment": [
                                        {
                                            "Name": 'OUTPUT_LOC',
                                            "Value": output_location
                                        }
                                    ]
                                }
                            ]
                        },
                        "tags": [{"key":"extra","value":extra},
                                {"key":"teamcluster","value":TeamCluster},
                                {"key":"autoscaling","value":Autoscaling},]
                }
                )
            else:
                try:
                    single_ecs_instance = config_yaml['general']['singleECSInstance']
                    instance_type=get_largest_instance_type(workflow_version_id)
                    print('largest inst in ecs',instance_type)
                    print(type(instance_type))
                    placementConstraint_expression = "attribute:ecs.instance-type =~" +" "+instance_type
                    print('placement const in ecs',placementConstraint_expression)
                    if single_ecs_instance:
                        step = EcsRunTaskStep(
                            state_id=state_id,
                            parameters={
                                "Payload":"qwertyuiop",
                                "Cluster": cluster_name,
                                "TaskDefinition": task_arn,
                                "PropagateTags": "TASK_DEFINITION",
                                "Overrides": {
                                    "ContainerOverrides": [
                                        {
                                            "Name": 'container1',
                                            "Environment": [
                                                {"Name": 'MODULE_LOC',"Value": codeLocation},
                                                {"Name": 'INPUT_LOC',"Value": input_location},
                                                {"Name": 'CONFIG_FILE',"Value": config_string}
                                            ]
                                        },
                                        {
                                            "Name": 'container3',
                                            "Environment": [
                                                {
                                                    "Name": 'OUTPUT_LOC',
                                                    "Value": output_location
                                                }
                                            ]
                                        }
                                    ]
                                },
                                "PlacementConstraints": [
                                    {
                                        "Expression": placementConstraint_expression,
                                        "Type": "memberOf"
                                    }
                                ],
                                "tags": [{"key":"extra","value":extra},
                                    {"key":"teamcluster","value":TeamCluster},
                                    {"key":"autoscaling","value":Autoscaling},
                                    {"key":"single_ecs_instance","value":single_ecs_instance},]
                            }
                    )
                    else:
                        step = EcsRunTaskStep(
                            state_id=state_id,
                            parameters={
                                "Cluster": cluster_name, 
                                "TaskDefinition": task_arn,
                                "PropagateTags": "TASK_DEFINITION",
                                "Overrides": {
                                    "ContainerOverrides": [
                                        {
                                            "Name": 'container1',
                                            "Environment": [
                                                {"Name": 'MODULE_LOC',"Value": codeLocation},
                                                {"Name": 'INPUT_LOC',"Value": input_location},
                                                {"Name": 'CONFIG_FILE',"Value": config_string}
                                            ]
                                        },
                                        {
                                            "Name": 'container3',
                                            "Environment": [
                                                {
                                                    "Name": 'OUTPUT_LOC',
                                                    "Value": output_location
                                                }
                                            ]
                                        }
                                    ]
                                },
    
                                "tags": [{"key":"extra","value":extra},
                                    {"key":"teamcluster","value":TeamCluster},
                                    {"key":"autoscaling","value":Autoscaling},
                                    {"key":"single_ecs_instance","value":single_ecs_instance},]
                            }

                        )

                except:
                    step = EcsRunTaskStep(
                        state_id=state_id,
                        parameters={
                            "Cluster": cluster_name,
                            "TaskDefinition": task_arn,
                            "PropagateTags": "TASK_DEFINITION",
                            "Overrides": {
                                "ContainerOverrides": [
                                    {
                                        "Name": 'container1',
                                        "Environment": [
                                            {"Name": 'MODULE_LOC',"Value": codeLocation},
                                            {"Name": 'INPUT_LOC',"Value": input_location},
                                            {"Name": 'CONFIG_FILE',"Value": config_string}
                                        ]
                                    },
                                    {
                                        "Name": 'container3',
                                        "Environment": [
                                            {
                                                "Name": 'OUTPUT_LOC',
                                                "Value": output_location
                                            }
                                        ]
                                    }
                                ]
                            },
   
                            "tags": [{"key":"extra","value":extra},
                                {"key":"teamcluster","value":TeamCluster},
                                {"key":"autoscaling","value":Autoscaling}
                                ]
                        }

                    )
        return step
