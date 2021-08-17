import uuid
from stepfunctions.steps import Pass, Wait, Choice, LambdaStep, ChoiceRule, Succeed, Fail, Chain
from AssembleStacks import build_cft_definition
import base64
import constants


def create_deploy_steps(config_yaml, workflow_execution_id, context):
    
    cft = build_cft_definition(config_yaml, context)
    return cft
    

