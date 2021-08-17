from stepfunctions.steps import *
import utils


class LambdaStepBuilder:

    def getStep(self, context, definition):
        metadata = definition.get('metaData')
        state_id = metadata['description']
        print("LambdaLoader", state_id)
        function_name = utils.resolvePlaceHolderValues(metadata['resource'], context)
        inputs = utils.resolvePlaceHoldersDict(definition['inputs'], context)
        outputs_dest = utils.resolvePlaceHoldersDict(definition['outputs']['dest'], context)
        params = {"input": inputs, "output": {"dest": outputs_dest}}
        # parameters = utils.resolvePlaceHoldersDict(definition['inputs'], context)
        step = LambdaStep(
            state_id=state_id,
            parameters={
                "FunctionName.$": function_name,
                "Payload": params
            }, result_path="$.results." + metadata['name']
        )
        return step
