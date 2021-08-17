import json

from stepfunctions.steps import EcsRunTaskStep

import utils
import constants

class ECSEC2StepBuilder:
    def getStep(self, context, module_config):
        module_id = module_config.get('platformModuleId')
        definition = module_config.get('config')
        config_string = module_config.get('config_string')
        module_name = module_config.get('module_name')

        container_uri = utils.get_container_uri(module_id)
        cpu = definition.get("deployment").get('config').get('CPU')
        memory = definition.get("deployment").get('config').get('Memory')
        launch_type = 'EC2' #one of FARGATE or EC2
        task_arn = utils.update_ecs_task(context, container_uri, launch_type, cpu, memory)
        #metadata = definition.get('metaData')
        state_id = module_name # metadata['description']
        print("Submitting ECS Task:", state_id)
        # step_name = metadata['resource']
        iploc = []
        for i in range(len(definition.get('input').get('source'))):
            iploc.append(utils.resolvePlaceHolderValues(definition.get('input').get('source')[i].get('filePath'),context))
        if definition.get('input').get('dictionary'):
            for i in range(len(definition.get('input').get('dictionary'))):
                iploc.append(utils.resolvePlaceHolderValues(definition.get('input').get('dictionary')[i].get('filePath'),context))
        input_location  = " ".join(iploc)
        #parameters = definition['input']['source'], context
        oploc = []
        for i in range(len(definition.get('output').get('dest'))):
            oploc.append(utils.resolvePlaceHolderValues(definition.get('output').get('dest')[i].get('filePath'),context))
        output_location  = " ".join(oploc)
        #codeLocation    = utils.resolvePlaceHolderValues(definition['deployment']['ArtifactLocation'],context)#+ "/" +definition['deployment']['name']
        codeLocation    = utils.get_artifact_location(module_id)
        # handler = definition['deployment']['handler']

        step = EcsRunTaskStep(
            state_id=state_id,
            parameters={
                # "capacityProviderStrategy": [
                #     {
                #         "capacityProvider": "BigDataCluster-CapacityProvider",
                #         "base": 0,
                #         "weight": 1
                #     }
                # ],
                "Cluster": "BigDataCluster", #This is to generated dynamically
                "TaskDefinition": task_arn,
                "PropagateTags": "TASK_DEFINITION",
                # "NetworkConfiguration": {
                #     "AwsvpcConfiguration": {
                #         # "AssignPublicIp": "DISABLED",
                #         "SecurityGroups": [
                #             'sg-049b29b3c04ad83b8' #This is to generated dynamically
                #         ],
                #         "Subnets": [
                #             'subnet-03e57d97fbcd5716f', 'subnet-0382cc37b36ee8853' #This is to generated dynamically
                #         ]
                #     }
                # },
                "PlacementConstraints": [
                    {
                        "Type": "distinctInstance"
                    }
                ],
                "Overrides": {
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
