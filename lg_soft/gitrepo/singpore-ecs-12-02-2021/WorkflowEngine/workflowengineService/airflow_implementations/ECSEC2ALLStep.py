import json

from stepfunctions.steps import EcsRunTaskStep

import utils
import constants

def fetch_config_from_db(config_id, type):
    return utils.fetch_config_from_db(config_id, type)
    
class ECSEC2ALLStepBuilder:
    def getStep(self, context, module_id, definition, config_file_location,workflow_version_id,workflow_module_id):
        print('--------------------------------------')
        print("context in ecs ec2 step:",context)
        print('---------------------------------------')
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
        team_name=context['tags']['team']
        cluster_name=team_name.replace(" ", "-")+'-cluster'
        print(cluster_name)



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
        #extra['deployment_steps'] = deployment_steps
        
        step = EcsRunTaskStep(
            state_id=state_id,
            parameters={
                "Cluster": "team1-cluster",
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
                        'key': 'team-inst',
                        'value': 'team1-t2.small'
                    },
                ]
            }

        )
        return step
