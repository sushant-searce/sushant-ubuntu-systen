import punq
import sys
import logging
import traceback
import constants
import time
import json
# import string

from airflow_implementations.LambdaStep import LambdaStepBuilder
from airflow_implementations.SageMakerStep import InferenceStepBuilder
from airflow_implementations.LGECSStep import LGECSStepBuilder
from airflow_implementations.EmrStep import EmrStepBuilder
from airflow_implementations.GlueStep import GlueStepBuilder
from airflow_implementations.ECSEC2Step import ECSEC2StepBuilder
# import pdb

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


class AirflowFactory:
    """
    In the init, adding the step builders to a container.
    """


    def __init__(self):
        container.register("function", LambdaStepBuilder)
        container.register("batch_inference", InferenceStepBuilder)
        container.register("ecs", LGECSStepBuilder)
        container.register("ecs-ec2", ECSEC2StepBuilder)
        container.register("emr", EmrStepBuilder)
        container.register("glue", GlueStepBuilder)
        container.register("dataloader", GlueStepBuilder)

    @staticmethod
    def getStepImplementation(context, module_config, workflow_version_id):
        try:
            definition = module_config.get('config')
            print('Airflow deployment type:',definition.get('deployment').get('type'))

            module_name = module_config.get('module_name')

            # Replacing special charaters in module description and limiting its size
            # definition['metaData']['description'] = definition['metaData']['description'].translate({ord(c): " " for c in "!@#$%^&*()[]{};:,./<>?\|`~-=_+"})[:55] + " " + str(round(time.time()*1000))[9:]
            # module_desc = definition['metaData']['description']
            # printable = set(string.printable)
            # definition['metaData']['description'] = ''.join(filter(lambda x: x in printable, module_desc))+ " " + str(round(time.time()*1000))[9:]

            # 1: Check if deployment type and configs are valid
            deployment = definition.get('deployment')
            if not deployment:
                return f"'deployment' field missing in {module_name} module config."
            if not deployment.get('type'):
                return f"'type' field missing in 'deployment' section of {module_name} module config."
            if deployment['type'] not in ["ecs", "ecs-ec2", "emr", "glue", "function"]:
                return f"Invalid deployment type for {module_name} module. Please choose one from ecs, ecs-ec2, emr, glue, function"
            # 1.1: ECS: Fargate and EC2
            elif deployment['type'] in ["ecs", "ecs-ec2"]:
                if deployment['type'] == "ecs" and deployment.get('config').get('launch_type') != 'FARGATE':
                    return f"'launch_type' incorrect/missing in deployment config of {module_name} module. Please use launch_type FARGATE"
                if deployment['type'] == "ecs-ec2":
                    if deployment.get('config').get('launch_type') != 'EC2':
                        return f"'launch_type' incorrect/missing in deployment config of {module_name} module. Please use launch_type EC2"
                    # if not deployment.get('config').get('ImageId'):
                    #     return f"'ImageId' missing in deployment config of {module_name} module"
                    # if not deployment.get('config').get('InstanceType'):
                    #     return f"'InstanceType' missing in deployment config of {module_name} module"
                    # allowed_instance_types = ['t1.micro', 't2.nano', 't2.micro', 't2.small', 't2.medium', 't2.large', 't2.xlarge', 't2.2xlarge', 't3.nano', 't3.micro', 't3.small', 't3.medium', 't3.large', 't3.xlarge', 't3.2xlarge', 't3a.nano', 't3a.micro', 't3a.small', 't3a.medium', 't3a.large', 't3a.xlarge', 't3a.2xlarge', 't4g.nano', 't4g.micro', 't4g.small', 't4g.medium', 't4g.large', 't4g.xlarge', 't4g.2xlarge', 'm1.small', 'm1.medium', 'm1.large', 'm1.xlarge', 'm3.medium', 'm3.large', 'm3.xlarge', 'm3.2xlarge', 'm4.large', 'm4.xlarge', 'm4.2xlarge', 'm4.4xlarge', 'm4.10xlarge', 'm4.16xlarge', 'm2.xlarge', 'm2.2xlarge', 'm2.4xlarge', 'cr1.8xlarge', 'r3.large', 'r3.xlarge', 'r3.2xlarge', 'r3.4xlarge', 'r3.8xlarge', 'r4.large', 'r4.xlarge', 'r4.2xlarge', 'r4.4xlarge', 'r4.8xlarge', 'r4.16xlarge', 'r5.large', 'r5.xlarge', 'r5.2xlarge', 'r5.4xlarge', 'r5.8xlarge', 'r5.12xlarge', 'r5.16xlarge', 'r5.24xlarge', 'r5.metal', 'r5a.large', 'r5a.xlarge', 'r5a.2xlarge', 'r5a.4xlarge', 'r5a.8xlarge', 'r5a.12xlarge', 'r5a.16xlarge', 'r5a.24xlarge', 'r5b.large', 'r5b.xlarge', 'r5b.2xlarge', 'r5b.4xlarge', 'r5b.8xlarge', 'r5b.12xlarge', 'r5b.16xlarge', 'r5b.24xlarge', 'r5b.metal', 'r5d.large', 'r5d.xlarge', 'r5d.2xlarge', 'r5d.4xlarge', 'r5d.8xlarge', 'r5d.12xlarge', 'r5d.16xlarge', 'r5d.24xlarge', 'r5d.metal', 'r5ad.large', 'r5ad.xlarge', 'r5ad.2xlarge', 'r5ad.4xlarge', 'r5ad.8xlarge', 'r5ad.12xlarge', 'r5ad.16xlarge', 'r5ad.24xlarge', 'r6g.metal', 'r6g.medium', 'r6g.large', 'r6g.xlarge', 'r6g.2xlarge', 'r6g.4xlarge', 'r6g.8xlarge', 'r6g.12xlarge', 'r6g.16xlarge', 'r6gd.metal', 'r6gd.medium', 'r6gd.large', 'r6gd.xlarge', 'r6gd.2xlarge', 'r6gd.4xlarge', 'r6gd.8xlarge', 'r6gd.12xlarge', 'r6gd.16xlarge', 'x1.16xlarge', 'x1.32xlarge', 'x1e.xlarge', 'x1e.2xlarge', 'x1e.4xlarge', 'x1e.8xlarge', 'x1e.16xlarge', 'x1e.32xlarge', 'i2.xlarge', 'i2.2xlarge', 'i2.4xlarge', 'i2.8xlarge', 'i3.large', 'i3.xlarge', 'i3.2xlarge', 'i3.4xlarge', 'i3.8xlarge', 'i3.16xlarge', 'i3.metal', 'i3en.large', 'i3en.xlarge', 'i3en.2xlarge', 'i3en.3xlarge', 'i3en.6xlarge', 'i3en.12xlarge', 'i3en.24xlarge', 'i3en.metal', 'hi1.4xlarge', 'hs1.8xlarge', 'c1.medium', 'c1.xlarge', 'c3.large', 'c3.xlarge', 'c3.2xlarge', 'c3.4xlarge', 'c3.8xlarge', 'c4.large', 'c4.xlarge', 'c4.2xlarge', 'c4.4xlarge', 'c4.8xlarge', 'c5.large', 'c5.xlarge', 'c5.2xlarge', 'c5.4xlarge', 'c5.9xlarge', 'c5.12xlarge', 'c5.18xlarge', 'c5.24xlarge', 'c5.metal', 'c5a.large', 'c5a.xlarge', 'c5a.2xlarge', 'c5a.4xlarge', 'c5a.8xlarge', 'c5a.12xlarge', 'c5a.16xlarge', 'c5a.24xlarge', 'c5ad.large', 'c5ad.xlarge', 'c5ad.2xlarge', 'c5ad.4xlarge', 'c5ad.8xlarge', 'c5ad.12xlarge', 'c5ad.16xlarge', 'c5ad.24xlarge', 'c5d.large', 'c5d.xlarge', 'c5d.2xlarge', 'c5d.4xlarge', 'c5d.9xlarge', 'c5d.12xlarge', 'c5d.18xlarge', 'c5d.24xlarge', 'c5d.metal', 'c5n.large', 'c5n.xlarge', 'c5n.2xlarge', 'c5n.4xlarge', 'c5n.9xlarge', 'c5n.18xlarge', 'c5n.metal', 'c6g.metal', 'c6g.medium', 'c6g.large', 'c6g.xlarge', 'c6g.2xlarge', 'c6g.4xlarge', 'c6g.8xlarge', 'c6g.12xlarge', 'c6g.16xlarge', 'c6gd.metal', 'c6gd.medium', 'c6gd.large', 'c6gd.xlarge', 'c6gd.2xlarge', 'c6gd.4xlarge', 'c6gd.8xlarge', 'c6gd.12xlarge', 'c6gd.16xlarge', 'c6gn.medium', 'c6gn.large', 'c6gn.xlarge', 'c6gn.2xlarge', 'c6gn.4xlarge', 'c6gn.8xlarge', 'c6gn.12xlarge', 'c6gn.16xlarge', 'cc1.4xlarge', 'cc2.8xlarge', 'g2.2xlarge', 'g2.8xlarge', 'g3.4xlarge', 'g3.8xlarge', 'g3.16xlarge', 'g3s.xlarge', 'g4ad.4xlarge', 'g4ad.8xlarge', 'g4ad.16xlarge', 'g4dn.xlarge', 'g4dn.2xlarge', 'g4dn.4xlarge', 'g4dn.8xlarge', 'g4dn.12xlarge', 'g4dn.16xlarge', 'g4dn.metal', 'cg1.4xlarge', 'p2.xlarge', 'p2.8xlarge', 'p2.16xlarge', 'p3.2xlarge', 'p3.8xlarge', 'p3.16xlarge', 'p3dn.24xlarge', 'p4d.24xlarge', 'd2.xlarge', 'd2.2xlarge', 'd2.4xlarge', 'd2.8xlarge', 'd3.xlarge', 'd3.2xlarge', 'd3.4xlarge', 'd3.8xlarge', 'd3en.xlarge', 'd3en.2xlarge', 'd3en.4xlarge', 'd3en.6xlarge', 'd3en.8xlarge', 'd3en.12xlarge', 'f1.2xlarge', 'f1.4xlarge', 'f1.16xlarge', 'm5.large', 'm5.xlarge', 'm5.2xlarge', 'm5.4xlarge', 'm5.8xlarge', 'm5.12xlarge', 'm5.16xlarge', 'm5.24xlarge', 'm5.metal', 'm5a.large', 'm5a.xlarge', 'm5a.2xlarge', 'm5a.4xlarge', 'm5a.8xlarge', 'm5a.12xlarge', 'm5a.16xlarge', 'm5a.24xlarge', 'm5d.large', 'm5d.xlarge', 'm5d.2xlarge', 'm5d.4xlarge', 'm5d.8xlarge', 'm5d.12xlarge', 'm5d.16xlarge', 'm5d.24xlarge', 'm5d.metal', 'm5ad.large', 'm5ad.xlarge', 'm5ad.2xlarge', 'm5ad.4xlarge', 'm5ad.8xlarge', 'm5ad.12xlarge', 'm5ad.16xlarge', 'm5ad.24xlarge', 'm5zn.large', 'm5zn.xlarge', 'm5zn.2xlarge', 'm5zn.3xlarge', 'm5zn.6xlarge', 'm5zn.12xlarge', 'm5zn.metal', 'h1.2xlarge', 'h1.4xlarge', 'h1.8xlarge', 'h1.16xlarge', 'z1d.large', 'z1d.xlarge', 'z1d.2xlarge', 'z1d.3xlarge', 'z1d.6xlarge', 'z1d.12xlarge', 'z1d.metal', 'u-6tb1.metal', 'u-9tb1.metal', 'u-12tb1.metal', 'u-18tb1.metal', 'u-24tb1.metal', 'a1.medium', 'a1.large', 'a1.xlarge', 'a1.2xlarge', 'a1.4xlarge', 'a1.metal', 'm5dn.large', 'm5dn.xlarge', 'm5dn.2xlarge', 'm5dn.4xlarge', 'm5dn.8xlarge', 'm5dn.12xlarge', 'm5dn.16xlarge', 'm5dn.24xlarge', 'm5n.large', 'm5n.xlarge', 'm5n.2xlarge', 'm5n.4xlarge', 'm5n.8xlarge', 'm5n.12xlarge', 'm5n.16xlarge', 'm5n.24xlarge', 'r5dn.large', 'r5dn.xlarge', 'r5dn.2xlarge', 'r5dn.4xlarge', 'r5dn.8xlarge', 'r5dn.12xlarge', 'r5dn.16xlarge', 'r5dn.24xlarge', 'r5n.large', 'r5n.xlarge', 'r5n.2xlarge', 'r5n.4xlarge', 'r5n.8xlarge', 'r5n.12xlarge', 'r5n.16xlarge', 'r5n.24xlarge', 'inf1.xlarge', 'inf1.2xlarge', 'inf1.6xlarge', 'inf1.24xlarge', 'm6g.metal', 'm6g.medium', 'm6g.large', 'm6g.xlarge', 'm6g.2xlarge', 'm6g.4xlarge', 'm6g.8xlarge', 'm6g.12xlarge', 'm6g.16xlarge', 'm6gd.metal', 'm6gd.medium', 'm6gd.large', 'm6gd.xlarge', 'm6gd.2xlarge', 'm6gd.4xlarge', 'm6gd.8xlarge', 'm6gd.12xlarge', 'm6gd.16xlarge', 'mac1.metal', 'x2gd.medium', 'x2gd.large', 'x2gd.xlarge', 'x2gd.2xlarge', 'x2gd.4xlarge', 'x2gd.8xlarge', 'x2gd.12xlarge', 'x2gd.16xlarge', 'x2gd.metal',]
                    # if deployment.get('config').get('InstanceType') not in allowed_instance_types:
                    #     return f"'InstanceType' incorrect in deployment config of {module_name} module"

                CPU = int(deployment['config']['CPU'])
                Memory = int(deployment['config']['Memory'])
                if not(256 <= CPU <= 4096) or (CPU % 256) != 0 :
                    return f"Value of CPU in {module_name} config is incorrect. Valid values are 256, 512, 1024, 2048 & 4096 (1024 being 1vCPU)"
                elif Memory >= 30720:
                    return f"Value of Memory in {module_name} config is incorrect. It should be less than or equal to 30720"
                elif CPU == 256:
                    if not(512 <= Memory <= 2048)  or (Memory % 512) != 0 :
                        return f"Value of Memory in {module_name} config is incorrect. Valid values are 512, 1024 & 2048"
                elif CPU == 512:
                    if not(1024 <= Memory <= 4096) or (Memory % 1024) != 0 :
                        return f"Value of Memory in {module_name} config is incorrect. Valid values are 1024, 2048, 3072 & 4096"
                elif CPU == 1024:
                    if not(2048 <= Memory <= 8192) or (Memory % 1024) != 0 :
                        return f"Value of Memory in {module_name} config is incorrect. Valid values are (2-8GB)x1024"
                elif CPU == 2048:
                    if not(4096 <= Memory <= 16384) or (Memory % 1024) != 0 :
                        return f"Value of Memory in {module_name} config is incorrect. Valid values are (4-16GB)x1024"
                elif CPU == 4096:
                    if not(8192 <= Memory <= 30720) or (Memory % 1024) != 0 :
                        return f"Value of Memory in {module_name} config is incorrect. Valid values are (8-30GB)x1024"
            # More deployment type validations to be added here

            # Check if bucket names and input/dictionary/output file names are right
            for input in definition['input']['source']:
                filePath = input['filePath']
                bucket_name = filePath.split('/')[2]
                if bucket_name != constants.SC_S3_BUCKET:
                    return f"Incorrect bucket name in one of the inputs of {module_name} module"
                input_file_name = filePath.split('/')[-1]
                if input_file_name != input["name"]:
                    return f"File name and path mismatch in one of the inputs of {module_name} module"

            if definition.get('input').get('dictionary'):
                for dict_input in definition['input']['dictionary']:
                    filePath = dict_input['filePath']
                    bucket_name = filePath.split('/')[2]
                    if bucket_name != constants.SC_S3_BUCKET:
                        return f"Incorrect bucket name in one of the inputs of {module_name} module"
                    dict_file_name = filePath.split('/')[-1]
                    if dict_file_name != dict_input["name"]:
                        return f"File name and path mismatch in one of the inputs of {module_name} module"

            for output in definition['output']['dest']:
                filePath = output['filePath']
                bucket_name = filePath.split('/')[2]
                if bucket_name != constants.SC_S3_BUCKET:
                    return f"Incorrect bucket name in one of the outputs of {module_name} module"
                output_file_name = filePath.split('/')[-1]
                if output_file_name != output["name"]:
                    return f"File name and path mismatch in one of the outputs of {module_name} module"

            step = container.resolve(definition.get('deployment').get('type')).getStep(context, module_config, workflow_version_id)
            if step is not None:
                return step
            else:
                return f"Error while creating deployment step for {module_name} module"
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
            return f"Caught exception while creating deployment step for {module_name} module"
            # raise
