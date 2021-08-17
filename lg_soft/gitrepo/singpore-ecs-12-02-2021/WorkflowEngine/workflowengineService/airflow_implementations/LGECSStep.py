import json

from stepfunctions.steps import EcsRunTaskStep

import utils
import constants

def fetch_config_from_db(config_id, type):
    return utils.fetch_config_from_db(config_id, type)


class LGECSStepBuilder:
    def getStep(self, context, module_id, definition, config_file_location,workflow_version_id,workflow_module_id):
        container_uri = utils.get_container_uri(module_id)
        launch_type=definition.get("deployment").get('config').get('launch_type')
        cpu = definition.get("deployment").get('config').get('CPU')
        memory = definition.get("deployment").get('config').get('Memory')
        task_arn = utils.update_ecs_task(context,container_uri, launch_type, cpu, memory)
        # task_arn = utils.update_ecs_task("797237262327.dkr.ecr.ap-southeast-1.amazonaws.com/lpp_modules_docker_repo:genericdoc")
        # task_arn = 'arn:aws:ecs:ap-southeast-1:797237262327:task-definition/ecsTask:5'

        metadata = definition.get('metaData')
        state_id = metadata['description']
        print("Submitting ECS Task:", state_id)
        # step_name = metadata['resource']
        #parameters = definition['input']['source'], context

        #input_file      = utils.resolvePlaceHolderValues(definition.get('input').get('source')[0].get("name"),context)
        iploc = []
        for i in range(len(definition.get('input').get('source'))):
            iploc.append(utils.resolvePlaceHolderValues(definition.get('input').get('source')[i].get('filePath'),context))
        #for i in range(len(definition.get('input').get('dictionary'))):
        #        iploc.append(utils.resolvePlaceHolderValues(definition.get('input').get('dictionary')[i].get('filePath'),context))
        input_location  = " ".join(iploc)
        #input_location  = utils.resolvePlaceHolderValues(definition.get('input').get('source')[0].get('filePath'),context) #+ "/" + parameters[0][0].get("FileName")
        #output_file     = utils.resolvePlaceHolderValues(definition.get("output").get('dest')[0].get('name'),context)
        oploc = []
        for i in range(len(definition.get('output').get('dest'))):
            oploc.append(utils.resolvePlaceHolderValues(definition.get('output').get('dest')[i].get('filePath'),context))
            print('oploc is:',oploc)

        output_location  = " ".join(oploc)
        #output_location = utils.resolvePlaceHolderValues(definition.get("output").get('dest')[0].get('filePath'),context) #+ "/" + definition.get("outputs").get('dest').get('FileName')
        #codeLocation    = utils.resolvePlaceHolderValues(definition['deployment']['ArtifactLocation'],context)#+ "/" +definition['deployment']['name']
        codeLocation    = utils.get_artifact_location(module_id)
        # handler = definition['deployment']['handler']
        # outputs_dest = definition['output']['dest']
        # print('output dest is:',outputs_dest)
        # print('---------------------------------')
        # params = {"input_location": input_location ,  "output": {"dest": outputs_dest}}
        # print('all prameters are:',params)
        # print('-------------------------------------')
        extra = {}
        config_yaml = fetch_config_from_db(workflow_version_id, constants.WORKFLOW_CONFIG_VERSION)
        modules = config_yaml.get('modules')
        for module in modules:
            if modules[module].get('id') == workflow_module_id:
                name = module
                break
        steps = config_yaml.get('steps')    
        extra['saveOutput'] = steps[name].get('saveOutput')
        extra['skipModule'] = steps[name].get('skipModule')
        
        step = EcsRunTaskStep(
            state_id=state_id,
            parameters={
                "LaunchType": "FARGATE",
                "Cluster": constants.FARGATE_CLUSTER,
                "TaskDefinition": task_arn,
                "NetworkConfiguration": {
                    "AwsvpcConfiguration": {
                        "AssignPublicIp": "ENABLED",
                        "SecurityGroups": [
                            constants.FARGATE_SEC_GROUP
                        ],
                        "Subnets": [
                            constants.FARGATE_SUBNET
                        ]
                    }
                }, "Overrides": {
                    "ContainerOverrides": [
                        {
                            "Name": 'container1',
                            "Environment": [
                                {
                                    "Name": 'MODULE_LOC',
                                    "Value": codeLocation
                                },
                                {
                                    "Name": 'INPUT_LOC',
                                    "Value": input_location
                                },
                                # {
                                #     "Name": 'INPUT_FILE',
                                #     "Value": input_file
                                # },
                                {
                                    "Name": 'CONFIG_FILE',
                                    "Value": config_file_location
                                }
                            ]
                        },
                        {
                            "Name": 'container3',
                            "Environment": [
                                # {
                                #     "Name": 'OUTPUT_FILE',
                                #     "Value": output_file
                                # },
                                {
                                    "Name": 'OUTPUT_LOC',
                                    "Value": output_location
                                }
                            ]
                        }
                    ]
                },
                "tags": [
                    {
                        "key":"extra",
                        "value":extra
                    },
                ]
            }

        )
        return step


