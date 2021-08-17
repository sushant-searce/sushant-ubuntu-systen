import sys
import logging
import traceback
import constants
import json
import punq

from implementations.ChoiceStep import ChoiceStepBuilder
from implementations.ECSStep import ECSStepBuilder
from implementations.LambdaStep import LambdaStepBuilder
from implementations.NotificationStep import NotificationStepBuilder
from implementations.ParallelStep import ParallelStepBuilder
from implementations.SageMakerStep import InferenceStepBuilder
from implementations.LGECSStep import LGECSStepBuilder
from implementations.ECSEC2Step import ECSEC2StepBuilder
from implementations.StringTestStep import StringTestStepBuilder

logger = logging.getLogger()
logger.setLevel(logging.INFO)

"""

punq is a simple DI library.  Th idea is create all the module implementations and store it in
a registry. Then inside the engine, other components can make use of this registry.

For eg:
container.register("lambda",LambdaImplementation)
container.resolve("lambda") -->  will return lambda implementation

"""

container = punq.Container()


class StepFactory:
    """
    In the init, adding the step builders to a container.
    """


    def __init__(self):
        container.register("function", LambdaStepBuilder)
        container.register("batch_inference", InferenceStepBuilder)
        container.register("notify_email", NotificationStepBuilder)
        container.register("choice", ChoiceStepBuilder)
        container.register("ecs", ECSStepBuilder)
        container.register("parallel", ParallelStepBuilder)
        container.register("fargate", LGECSStepBuilder)
        container.register("ecs-ec2", ECSEC2StepBuilder)
        container.register("stringTest",StringTestStepBuilder)

    @staticmethod
    def getStepImplementation(context, module_config):
        try:
            definition = module_config.get('config')
            if context['stringInputs']:
                definition['deployment']['type'] = "stringTest"

            # Check if deployment types are valid
            deployment = definition['deployment']
            if deployment['type'] not in ["fargate", "ecs-ec2", "stringTest"]:
                return "Invalid deployment type. Please choose one from fargate, ecs-ec2"
            elif deployment['type'] in ["fargate", "ecs-ec2"]:
                CPU = int(deployment['config']['CPU'])
                Memory = int(deployment['config']['Memory'])
                if not(256 <= CPU <= 4096) or (CPU % 256) != 0 :
                    return f"Value of CPU in {module_config.get('module_name')} config is incorrect. Valid values are 256, 512, 1024, 2048 & 4096 (1024 being 1vCPU)"
                elif Memory >= 30720:
                    return f"Value of Memory in {module_config.get('module_name')} config is incorrect. It should be less than or equal to 30720"
                elif CPU == 256:
                    if not(512 <= Memory <= 2048)  or (Memory % 512) != 0 :
                        return f"Value of Memory in {module_config.get('module_name')} config is incorrect. Valid values are 512, 1024 & 2048"
                elif CPU == 512:
                    if not(1024 <= Memory <= 4096) or (Memory % 1024) != 0 :
                        return f"Value of Memory in {module_config.get('module_name')} config is incorrect. Valid values are 1024, 2048, 3072 & 4096"
                elif CPU == 1024:
                    if not(2048 <= Memory <= 8192) or (Memory % 1024) != 0 :
                        return f"Value of Memory in {module_config.get('module_name')} config is incorrect. Valid values are (2-8GB)x1024"
                elif CPU == 2048:
                    if not(4096 <= Memory <= 16384) or (Memory % 1024) != 0 :
                        return f"Value of Memory in {module_config.get('module_name')} config is incorrect. Valid values are (4-16GB)x1024"
                elif CPU == 4096:
                    if not(8192 <= Memory <= 30720) or (Memory % 1024) != 0 :
                        return f"Value of Memory in {module_config.get('module_name')} config is incorrect. Valid values are (8-30GB)x1024"
            # More deployment type validations to be added here

            # Check if bucket names and input/dictionary/output file names are right
            for input in definition['input']['source']:
                filePath = input['filePath']
                bucket_name = filePath.split('/')[2]
                if bucket_name != constants.SC_S3_BUCKET:
                    return f"Incorrect bucket name in one of the inputs of {module_config.get('module_name')} module"
                input_file_name = filePath.split('/')[-1]
                if input_file_name != input["name"]:
                    return f"File name and path mismatch in one of the inputs of {module_config.get('module_name')} module"

            if definition.get('input').get('dictionary'):
                for dict_input in definition['input']['dictionary']:
                    filePath = dict_input['filePath']
                    bucket_name = filePath.split('/')[2]
                    if bucket_name != constants.SC_S3_BUCKET:
                        return f"Incorrect bucket name in one of the inputs of {module_config.get('module_name')} module"
                    dict_file_name = filePath.split('/')[-1]
                    if dict_file_name != dict_input["name"]:
                        return f"File name and path mismatch in one of the inputs of {module_config.get('module_name')} module"

            for output in definition['output']['dest']:
                filePath = output['filePath']
                bucket_name = filePath.split('/')[2]
                if bucket_name != constants.SC_S3_BUCKET:
                    return f"Incorrect bucket name in one of the outputs of {module_config.get('module_name')} module"
                output_file_name = filePath.split('/')[-1]
                if output_file_name != output["name"]:
                    return f"File name and path mismatch in one of the outputs of {module_config.get('module_name')} module"

            step = container.resolve(definition.get('deployment').get('type')).getStep(context, module_config)
            if step is not None:
                return step
            else:
                return f"Error while creating step for deployment type {definition['deployment']['type']}"

        except Exception as e:
            # print("Un-supported Operation",e)
            exception_type, exception_value, exception_traceback = sys.exc_info()
            traceback_string = traceback.format_exception(exception_type, exception_value, exception_traceback)
            err_msg = json.dumps({
              "errorType": exception_type.__name__,
              "errorMessage": str(exception_value),
              "stackTrace": traceback_string
            })
            logger.error(err_msg)
            return f"Caught exception while creating step for deployment type {definition['deployment']['type']}"
            # raise
