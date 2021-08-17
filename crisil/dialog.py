import os
import time
import json
import dialogflow
import dialogflow_v2beta1
import argparse

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/sushantnigudkar/Documents/key.json"

project_id = 'searce-playground'
session_id = "101"



def list_intents(project_id):
    import dialogflow_v2 as dialogflow
    intents_client = dialogflow.IntentsClient()

    parent = intents_client.project_agent_path(project_id)

    intents = intents_client.list_intents(parent)

    for intent in intents:
        print('=' * 20)
        print('Intent name: {}'.format(intent.name))
        print('Intent display_name: {}'.format(intent.display_name))
        print('Action: {}\n'.format(intent.action))
        print('Root followup intent: {}'.format(
            intent.root_followup_intent_name))
        print('Parent followup intent: {}\n'.format(
            intent.parent_followup_intent_name))

        print('Input contexts:')
        for input_context_name in intent.input_context_names:
            print('\tName: {}'.format(input_context_name))

        print('Output contexts:')
        for output_context in intent.output_contexts:
            print('\tName: {}'.format(output_context.name))

list_intents(project_id)