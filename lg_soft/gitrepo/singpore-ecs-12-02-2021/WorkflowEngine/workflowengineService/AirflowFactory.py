import punq
from airflow_implementations.LambdaStep import LambdaStepBuilder
from airflow_implementations.NotificationStep import NotificationStepBuilder
from airflow_implementations.SageMakerStep import InferenceStepBuilder
from airflow_implementations.LGECSStep import LGECSStepBuilder
from airflow_implementations.EmrStep import EmrStepBuilder
from airflow_implementations.GlueStep import GlueStepBuilder
from airflow_implementations.ECSEC2Step import ECSEC2StepBuilder
from airflow_implementations.ECSEC2ALLStep import ECSEC2ALLStepBuilder

import pdb
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
        container.register("notify_email", NotificationStepBuilder)
        container.register("ecs", LGECSStepBuilder)
        container.register("ecs-ec2", ECSEC2StepBuilder)
        #container.register("ecs-ec2", ECSEC2ALLStepBuilder)
        container.register("emr", EmrStepBuilder)
        container.register("glue", GlueStepBuilder)

    @staticmethod
    def getStepImplementation(context, module_id, definition, config_file_location, workflow_version_id,workflow_module_id):
        try:
            print('airflow deployment type',definition.get('deployment').get('type'))
            step = container.resolve(definition.get('deployment').get('type')).getStep(context, module_id, definition, config_file_location, workflow_version_id,workflow_module_id)
            if step is not None:
                return step
        except Exception as e:
            print("Un-supported Operation",e)
            raise

