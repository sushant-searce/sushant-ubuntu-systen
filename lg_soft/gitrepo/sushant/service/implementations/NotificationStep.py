import utils
from stepfunctions.steps.service import SnsPublishStep
import yaml

class NotificationStepBuilder:

    def getStep(self, context, definition):
        metadata = definition.get('metaData')
        state_id = metadata.get("description")
        print("Notification State Id", state_id)
        parameters = utils.resolvePlaceHoldersDict(definition['inputs'], context)
        topic_arn = utils.resolvePipelineVariables(definition.get('deployment').get('TopicArn'),context)
        message = parameters.get('Message')

        return SnsPublishStep(state_id=state_id, parameters={
            'TopicArn': topic_arn,
            'Message': message
        },result_path="$.results."+metadata['name'])
