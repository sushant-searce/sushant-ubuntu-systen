import json

from stepfunctions.steps import EcsRunTaskStep

import utils
import constants

class LGECSStepBuilder:
    def getStep(self, context, module_config):
        module_id = module_config.get('platformModuleId')
        definition = module_config.get('config')
        config_string = module_config.get('config_string')
        module_name = module_config.get('module_name')

        container_uri = utils.get_container_uri(module_id)
        cpu = definition.get("deployment").get('config').get('CPU')
        memory = definition.get("deployment").get('config').get('Memory')
        launch_type = 'FARGATE' #one of FARGATE or EC2
        task_arn = utils.update_ecs_task(context, container_uri, launch_type, cpu, memory)
        # metadata = definition.get('metaData')
        state_id = module_name # metadata['description']


        iploc = []
        for i in range(len(definition.get('input').get('source'))):
            iploc.append(utils.resolvePlaceHolderValues(definition.get('input').get('source')[i].get('filePath'),context))
        if definition.get('input').get('dictionary'):
            for i in range(len(definition.get('input').get('dictionary'))):
                iploc.append(utils.resolvePlaceHolderValues(definition.get('input').get('dictionary')[i].get('filePath'),context))
        input_location  = " ".join(iploc)

        oploc = []
        for i in range(len(definition.get('output').get('dest'))):
            oploc.append(utils.resolvePlaceHolderValues(definition.get('output').get('dest')[i].get('filePath'),context))
        output_location  = " ".join(oploc)

        # output_file     = utils.resolvePlaceHolderValues(definition.get("output").get('dest')[0].get('name'),context)
        #codeLocation    = utils.resolvePlaceHolderValues(definition['deployment']['ArtifactLocation'],context)#+ "/" +definition['deployment']['name']
        codeLocation    = utils.get_artifact_location(module_id)
        # handler = definition['deployment']['handler']

        step = EcsRunTaskStep(
            state_id=state_id,
            parameters={
                "LaunchType": "FARGATE",
                "Cluster": constants.FARGATE_CLUSTER,
                "TaskDefinition": task_arn,
                "PropagateTags": "TASK_DEFINITION",
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
                                    "Value": config_string
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
                }
            }

        )
        return step
