import json

from stepfunctions.steps import EcsRunTaskStep

import utils
import constants


class ECSStepBuilder:
    def getStep(self, context, definition):
        metadata = definition.get('metaData')
        state_id = metadata['description']
        print("ECS Loader", state_id)
        step_name = metadata['resource']
        parameters = definition['inputs'], context

        ecs_parameters = {}
        ecs_parameters['file_location_1'] = utils.resolvePlaceHolderValues(parameters[0][0].get('filePath'),context) + "/" + parameters[0][0].get("FileName")
        ecs_parameters['file_location_2'] = utils.resolvePlaceHolderValues(parameters[0][1].get('filePath'),context) + "/" + parameters[0][1].get("FileName")
        ecs_parameters['output_location'] = utils.resolvePlaceHolderValues(definition.get("outputs").get('dest').get('filePath'),context) + "/" + definition.get("outputs").get('dest').get('FileName')

        codeLocation = utils.resolvePlaceHolderValues(definition['deployment']['ArtifactLocation'],context)
        handler = definition['deployment']['handler']

        step = EcsRunTaskStep(
            state_id=state_id,
            parameters={
                "LaunchType": "FARGATE",
                "Cluster": constants.FARGATE_CLUSTER,
                "TaskDefinition": constants.FARGATE_TASK_DEF,
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
                            "Name": constants.FARGATE_CONTAINER_NAME,
                            "Command": ["DynamicScriptRunner.py", codeLocation, handler, json.dumps(ecs_parameters)]
                        }
                    ]
                }
            }

        )
        return step
