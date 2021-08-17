from stepfunctions.steps import *
# import yaml
import utils
from sagemaker import transformer
import uuid


class InferenceStepBuilder:
    def getStep(self, context, definition):
        parameters = utils.resolvePlaceHoldersDict(definition['inputs'], context)
        model_name = utils.resolvePlaceHolderValues(definition['deployment']['model_name'], context)
        instance_count =  1
        instance_type = parameters.get('instance_type') or 'ml.m5.xlarge'
        assemble_with = parameters.get('assemble_with') or 'Line'
        max_payload = parameters.get('max_payload') or None
        output_path =  utils.resolvePlaceHolderValues(definition['outputs']['dest']['filePath'],context)
        input_path = parameters.get('FilePath.$') or parameters.get('filePath')  #
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
            job_name="States.Format('" + definition.get(metaData).get('name') + "-{}', $$.Execution.Name)",
            model_name=model_name,
            input_filter='$[1:]',
            data=input_path,
            content_type=content_type,
            result_path="$.results." + definition.get(metaData).get('name')
        )

        return transform_step


# class S3CSVDataLoader:
#     def output(self):
#         return {
#             'S3OutputArtifactUri': "s3://output",
#             'ContentType': 'text/csv'
#         }


# def main():
#     from sagemaker import Session
#     import yaml
#
#     context = {
#         'pipeline_execution': {
#             'BaseLocation': 's3://my_bucket_name',
#             'sagemaker_session': Session()
#         },
#         'step_execution': {
#             'DataLoader': S3CSVDataLoader()
#         }
#
#     }
#     with open('../config/batch_inference_1234.yaml', 'r') as f:
#         definition = yaml.full_load(f)
#         step = InferenceStepBuilder.getStep(None, context, definition)
#         print(step)
#
#
# main()
