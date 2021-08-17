from stepfunctions.steps import *
# import yaml
import utils
from sagemaker import transformer
import uuid


class InferenceStepBuilder:
    
    def getStep(self, context, module_config, workflow_version_id):
        module_id = module_config.get('platformModuleId')
        definition = module_config.get('config')
        config_string = module_config.get('config_string')
        module_name = module_config.get('module_name')
        workflow_module_id = module_config.get('module_id')
        
        parameters = utils.resolvePlaceHoldersDict(definition['input']['source'][0], context)
        model_name = utils.resolvePlaceHolderValues(definition['deployment']['config']['model_name'], context)
        instance_count =  1
        instance_type = parameters.get('instance_type') or 'ml.m5.xlarge'
        assemble_with = parameters.get('assemble_with') or 'Line'
        max_payload = parameters.get('max_payload') or None
        output_path =  utils.resolvePlaceHolderValues(definition['output']['dest'][0]['filePath'],context)
        input_path = parameters.get('filePath.$') or parameters.get('filePath')  
        step_id = definition.get('metaData').get('description')
        print("SageMaker Step",step_id)
        job_name =  str(uuid.uuid1())
        content_type = parameters.get('ContentType') or 'text/csv'
        transformer_object = transformer.Transformer(
            model_name=model_name,
            instance_count=instance_count,
            instance_type=instance_type,
            strategy='SingleRecord',
            assemble_with=assemble_with,
            max_payload=max_payload,
            output_path=output_path,
            sagemaker_session=context['pipeline_execution']['sagemaker_session'],
            accept=content_type)

        transform_step = TransformStep(
            state_id=step_id,
            transformer=transformer_object,
            split_type=assemble_with,
            job_name="States.Format('" + definition.get('metaData').get('name') + "-{}', $$.Execution.Name)",
            model_name=model_name,
            input_filter='$[1:]',
            data=input_path,
            content_type=content_type,
            result_path="$.results." + definition.get('metaData').get('name')
        )
    
        return transform_step
