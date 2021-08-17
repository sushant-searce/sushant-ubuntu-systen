import json

from stepfunctions.steps import EcsRunTaskStep

import utils
import constants

def fetch_config_from_db(config_id, type):
    return utils.fetch_config_from_db(config_id, type)
    
class ECSEC2StepBuilder:
    def getStep(self, context, module_id, definition, config_file_location,workflow_version_id,workflow_module_id , reuse_ecs_instance):
        print('reuse value from ecs-ec2 step',reuse_ecs_instance)
        container_uri = utils.get_container_uri(module_id)
        cpu = definition.get("deployment").get('config').get('CPU')
        memory = definition.get("deployment").get('config').get('Memory')
        launch_type=definition.get("deployment").get('config').get('launch_type')
        print(launch_type)
        task_arn = utils.update_ecs_task(context,container_uri, launch_type, cpu, memory)
        metadata = definition.get('metaData')
        state_id = metadata['description']

        # input_file      = utils.resolvePlaceHolderValues(definition.get('input').get('source')[0].get("name"),context)
        iploc = []
        for i in range(len(definition.get('input').get('source'))):
            iploc.append(utils.resolvePlaceHolderValues(definition.get('input').get('source')[i].get('filePath'),context))
        if definition.get('input').get('dictionary'):
            for i in range(len(definition.get('input').get('dictionary'))):
                iploc.append(utils.resolvePlaceHolderValues(definition.get('input').get('dictionary')[i].get('filePath'),context))
        input_location  = " ".join(iploc)
        output_file     = utils.resolvePlaceHolderValues(definition.get("output").get('dest')[0].get('name'),context)
        #output_location = utils.resolvePlaceHolderValues(definition.get("output").get('dest')[0].get('filePath'),context)
        oploc = []
        for i in range(len(definition.get('output').get('dest'))):
            oploc.append(utils.resolvePlaceHolderValues(definition.get('output').get('dest')[i].get('filePath'),context))
            print('oploc is:',oploc)

        output_location  = " ".join(oploc)
        codeLocation    = utils.get_artifact_location(module_id)

        instance_type= definition.get("deployment").get('config').get('InstanceType')
        print(instance_type)
        placementConstraint_expression = "attribute:ecs.instance-type =~" +" "+instance_type
        print(placementConstraint_expression)

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

        TeamCluster = True
        Autoscaling = False

        if not TeamCluster:
            step = EcsRunTaskStep(
                state_id=state_id,
                parameters={
                    "Cluster": "ecs-ec2", #This is to generated dynamically
                    "TaskDefinition": task_arn,
                    "PropagateTags": "TASK_DEFINITION",
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
                                    {
                                        "Name": 'CONFIG_FILE',
                                        "Value": config_file_location
                                    }
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
                    "tags": [
                        {
                            "key":"extra",
                            "value":extra
                        },
                        {
                            "key":"teamcluster",
                            "value":TeamCluster

                        },
                        {
                            "key":"autoscaling",
                            "value":Autoscaling

                        },
                        {
                            "key":"reuse_ecs_instance",
                            "value":reuse_ecs_instance

                        },
                    ]
                }

            )
        else:
            if Autoscaling:
                step = EcsRunTaskStep(
                    state_id=state_id,
                    parameters={
                        "Cluster": "BigDataCluster", #This is to generated dynamically
                        "TaskDefinition": task_arn,
                        "PropagateTags": "TASK_DEFINITION",
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
                                        {
                                            "Name": 'CONFIG_FILE',
                                            "Value": config_file_location
                                        }
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
                        "tags": [
                        {
                            "key":"extra",
                            "value":extra
                        },
                        {
                            "key":"teamcluster",
                            "value":TeamCluster

                        },
                        {
                            "key":"autoscaling",
                            "value":Autoscaling

                        },
                        {
                            "key":"reuse_ecs_instance",
                            "value":reuse_ecs_instance

                        },
                    ]
                }

            )
            
            else:
                if reuse_ecs_instance:
                    step = EcsRunTaskStep(
                        state_id=state_id,
                        parameters={
                            "Cluster": "team1-cluster", #This is to generated dynamically
                            "TaskDefinition": task_arn,
                            "PropagateTags": "TASK_DEFINITION",
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
                                            {
                                                "Name": 'CONFIG_FILE',
                                                "Value": config_file_location
                                            }
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
                            "placementConstraints": [
                                {
                                    #"expression": "attribute:ecs.instance-type =~  t2.micro",
                                    "expression": placementConstraint_expression,
                                    "type": "memberOf"
                                }
                            ],
                            "tags": [
                                {
                                    "key":"extra",
                                    "value":extra
                                },
                                {
                                    "key":"teamcluster",
                                    "value":TeamCluster

                                },
                                {
                                    "key":"autoscaling",
                                    "value":Autoscaling

                                },
                                {
                                    "key":"reuse_ecs_instance",
                                    "value":reuse_ecs_instance

                                },
                            ]
                        }

                    )
                else:
                    step = EcsRunTaskStep(
                        state_id=state_id,
                        parameters={
                            "Cluster": "team1-cluster", #This is to generated dynamically
                            "TaskDefinition": task_arn,
                            "PropagateTags": "TASK_DEFINITION",
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
                                            {
                                                "Name": 'CONFIG_FILE',
                                                "Value": config_file_location
                                            }
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
                            # "placementConstraints": [
                            #     {
                            #         #"expression": "attribute:ecs.instance-type =~  t2.micro",
                            #         "expression": placementConstraint_expression,
                            #         "type": "memberOf"
                            #     }
                            # ],
                            "tags": [
                                {
                                    "key":"extra",
                                    "value":extra
                                },
                                {
                                    "key":"teamcluster",
                                    "value":TeamCluster

                                },
                                {
                                    "key":"autoscaling",
                                    "value":Autoscaling

                                },
                                {
                                    "key":"reuse_ecs_instance",
                                    "value":reuse_ecs_instance

                                },
                            ]
                        }

                    )

        return step


