import uuid
from stepfunctions.steps import Pass, Wait, Choice, LambdaStep, ChoiceRule, Succeed, Fail, Chain
from AssembleStacks import build_cft_definition
import base64
import constants

def create_deployment_steps(config_yaml, workflow_execution_id, context):
    cft = build_cft_definition(config_yaml, context)

    create_resources_step = None
    wait_for_completion = None
    get_status = None
    delete_job = None

    # if cft.to_dict()['Resources']:
    cft_definition = cft.to_yaml()
    print(cft_definition)
    #print("cftdef:",cft_definition)
    #print("---------")
    encoded = base64.b64encode(cft_definition.encode('ascii')).decode('ascii')
    #stack_name = 'cft-' + str(uuid.uuid4())[:8]

    create_resources_step = LambdaStep(state_id="Creating Resources", parameters={
        "FunctionName": constants.CREATE_RESOURCE,
        "Payload": {
            "stackName.$": "States.Format('cft-{}', $$.Execution.Name)",
            "cft_template": encoded
        }
    })

    wait_for_completion = Wait(state_id="Wait for Resources to be Created", seconds=60)

    get_status = LambdaStep(state_id="Get Resource Creation Status", parameters={
        "FunctionName": constants.GET_CFT_STATUS,
        "Payload": {
            "stackName.$": "States.Format('cft-{}', $$.Execution.Name)"
        }
    })

    delete_job = LambdaStep(state_id="Deleting Resources", parameters={
        "FunctionName": constants.DELETE_RESOURCE,
        "Payload": {
            "stackName.$": "States.Format('cft-{}', $$.Execution.Name)"
            }
    })

    finishing_job = LambdaStep(state_id="Finishing Up", parameters={
        "FunctionName": constants.FINAL_STEP,
        "Payload": {
            "workflow_execution_id": workflow_execution_id
            }
        })

    return {
        "create": create_resources_step,
        "delete": delete_job,
        "get_status": get_status,
        "wait": wait_for_completion,
        "finish": finishing_job
    }

