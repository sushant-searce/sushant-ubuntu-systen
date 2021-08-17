from stepfunctions.steps import *
import utils
import constants
import time
import yaml
from subprocess import call
import argparse

def fetch_config_from_db(config_id, type):
    return utils.fetch_config_from_db(config_id, type)

def decode_yaml(data_loaded_config, module_id):
    data_loaded= yaml.safe_load(data_loaded_config)
    python_file = utils.get_artifact_location(module_id)+"/"+data_loaded['general']['scriptFile']
    inputs = data_loaded['input']['source']
    outputs = data_loaded["output"]["dest"]
    process = data_loaded["functional"]["process"]
    out = [python_file, inputs, outputs, process]
    return out

def cmds_creator(python_file, inputs, outputs, process, cmds = []):
    ips = []
    output = []
    # input_args = []
    # output_args = []
    if cmds == []:
        for i in inputs:
            ips.append([i["argName"], i["filePath"]])
        for j in outputs:
            output.append([j["argName"], j["filePath"]])
        temp_cmd = [] # creates 'python foo.py'
        for i in range(len(ips)):
            temp_cmd.append('--{}'.format(ips[i][0]))
            temp_cmd.append(ips[i][1])
        for i in range(len(output)):
            temp_cmd.append('--{}'.format(output[i][0]))
            temp_cmd.append(output[i][1])
        for key in process.keys():
            if 'dataType' in process[key]: # not a subparser
                temp_cmd.append('--{}'.format(key))
                if 'value' in process[key]:
                    temp_cmd.append(str(process[key]['value']))
            else: # a subparser
                temp_cmd.append(key)
                return cmds_creator(python_file,inputs,outputs,process[key],temp_cmd)
        cmds+=temp_cmd
        return [cmds]
    else:
        temp_cmd = []
        for key in process.keys():
            if 'dataType' in process[key]: # not a subparser
                temp_cmd.append('--{}'.format(key))
                if 'value' in process[key]:
                    temp_cmd.append(str(process[key]['value']))
            else: # a subparser
                temp_cmd.append(key)
                return cmds_creator(python_file,inputs,outputs,process[key],temp_cmd)
        cmds+=temp_cmd
        return [cmds]


class GlueStepBuilder:

    def getStep(self, context, module_id, definition, config_file_location,workflow_version_id,workflow_module_id):
        extra = {}
        python_file, inputs, outputs, process = decode_yaml(config_file_location,module_id)
        cmds = cmds_creator(python_file, inputs, outputs, process)
       # print('commands', cmds)

        extra = {}
        config_yaml = fetch_config_from_db(workflow_version_id, constants.WORKFLOW_CONFIG_VERSION)
        modules = config_yaml.get('modules')
        for module in modules:
            if modules[module].get('id') == workflow_module_id:
                name = module
                break
        steps = config_yaml.get('steps')
        extra['saveOutput'] = steps[name].get('saveOutput')
        extra['skipModule'] = steps[name].get('skipModule')
        metadata = definition.get('metaData')
        state_id = metadata.get('description')
        print("Glue", state_id)
        jobName = definition.get('deployment').get('config').get('jobName')

        jobScriptLocation = definition.get('deployment').get('config').get('jobScriptLocation')
        jobMaxCapacity = definition.get('deployment').get('config').get('jobMaxCapacity')


        oploc = []
        for i in range(len(definition.get('output').get('dest'))):
            oploc.append(utils.resolvePlaceHolderValues(definition.get('output').get('dest')[i].get('filePath'),context))
            print('oploc is:',oploc)
        outputs_dest = " ".join(oploc)

        dest = definition.get('output').get('dest')

        codeLocation    = utils.get_artifact_location(module_id)+"/"+definition['general']['scriptFile']
        
        jobType = definition['deployment']['config']['jobType']
        params = {"jobType": jobType, "jobScriptLocation": codeLocation, "jobMaxCapacity": jobMaxCapacity, "arguments": cmds, "output": {"dest": outputs_dest}, "extra":extra, "dest": dest}
        

        step = GlueStartJobRunStep(
            state_id=state_id,
            parameters={
                "jobName.$": jobName,
                "Payload": params
            }#, result_path="$.results." + metadata['name']
        )
        return step

