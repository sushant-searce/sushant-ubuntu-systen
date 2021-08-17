import yaml
import StepFactory
from stepfunctions.steps import Parallel, Pass
import constants
import utils


def fetch_config_from_db(config_id, module_type):
    return utils.fetch_config_from_db(config_id, module_type)
    # with open('./config-new/' + config_id + '.yaml') as file:
    #     return yaml.full_load(file)


class ParallelStepBuilder:

    def getStep(self, context, definition):
        metadata = definition.get('metaData')
        state_id = metadata['description']
        step = Parallel(state_id=state_id)
        branches = definition['inputs']['Branch']
        for branch_definition in branches:
            name = branch_definition['Name']
            sub_step_definition = definition.get('modules').get(name)
            module_id = sub_step_definition.get("id")
            step_config = fetch_config_from_db(module_id, constants.MODULE_CONFIG_VERSION)
            branch_step = StepFactory.StepFactory().getStepImplementation(context, step_config)
            step.add_branch(branch_step)
        return step
