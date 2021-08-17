import yaml
import StepFactory

from stepfunctions.steps import Choice, ChoiceRule, Pass
import constants
import utils

"""
Potential Workaround for creating Chainable Choice States. 
"""


class ChainableChoice(Choice):
    def next(self, next_step):
        self.default_choice(next_step)
        return next_step


def fetch_config_from_db(config_id, module_type):
    return utils.fetch_config_from_db(config_id, module_type)
    # with open('./config-new/' + config_id + '.yaml') as file:
    #     return yaml.full_load(file)


class ChoiceStepBuilder:
    def getStep(self, context, definition):
        metadata = definition.get('metaData')
        print("Choice State", metadata.get('description'))
        choice_state = ChainableChoice(
            state_id=metadata.get('description')
        )
        conditions = definition.get('inputs')
        for condition in conditions:
            resolved_condition = condition['Condition']
            variable = utils.resolveStepVariables(resolved_condition.get('Variable'), context)
            expression = resolved_condition.get('Expression')
            value = resolved_condition.get('ExpressionValue')

            next_implementation = None
            if condition['Condition'].get('next'):
                sub_step_definition = definition.get('modules').get(resolved_condition.get('next'))
                # next_implementation = context['step_execution'][sub_step_definition]
                module_id = sub_step_definition.get("id")
                next_implementation = None
                if module_id is not None:
                    step_config = fetch_config_from_db(module_id, constants.MODULE_CONFIG_VERSION)
                    next_implementation = StepFactory.StepFactory().getStepImplementation(context, step_config)
            choice_rule = getattr(ChoiceRule, expression)(variable=variable, value=value)
            choice_state.add_choice(choice_rule, next_step=next_implementation)

        return choice_state
