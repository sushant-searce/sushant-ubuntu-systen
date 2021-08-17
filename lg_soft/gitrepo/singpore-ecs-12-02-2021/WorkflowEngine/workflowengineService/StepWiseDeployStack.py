import uuid
from stepfunctions.steps import Pass, Wait, Choice, LambdaStep, ChoiceRule, Succeed, Fail, Chain
import base64
import constants

'''
Helper class to manage managing the resources needed for ML OPS dynamically
Takes in Deployment YAML as input.
It makes use of build_cft_definition() method to dynamically create a CFT based on deployment yaml.
It then sets up Lambda steps that can be executed for Creating, Deleting, Wait and Getting Status of deployments
of a CFT deployment.

'''
def test_build_cft(module_type, workflow_execution_id ,cft):

    create_resources_step = {}
    wait_for_completion = {}
    get_status = {}
    delete_job = {}


    if cft.to_dict()['Resources']:
        cft_definition = cft.to_yaml()
        encoded = base64.b64encode(cft_definition.encode('ascii')).decode('ascii')
        uid = uuid.uuid4().hex[:4]
        
        stack_id = "cft-"+uid
        
        create_resources_step["FunctionName"] = constants.CREATE_RESOURCE,
        create_resources_step["stackName"] = stack_id
        create_resources_step["cft_template"] = encoded
        delete_job["FunctionName"] = constants.DELETE_RESOURCE
        delete_job["stackName"] = stack_id

    deployment_steps = {}
    deployment_steps["Creating_Resources"] = create_resources_step
    deployment_steps["Deleting_Resources"] = delete_job
    # steps_list = []
    # steps_list.append(deployment_steps['create'])
    # steps_list.append(deployment_steps['wait'])
    # steps_list.append(deployment_steps['get_status'])

    return deployment_steps

