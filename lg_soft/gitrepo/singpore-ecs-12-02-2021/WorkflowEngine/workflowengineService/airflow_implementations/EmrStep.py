from stepfunctions.steps import *
import utils
import time
import yaml
from subprocess import call
import argparse
import constants
import base64
from zipfile import ZipFile
from os.path import basename
import os,sys,inspect
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
sys.path.insert(0, current_dir) 
from Assemble import build_emr_stack
from troposphere import Parameter, Ref, Template, GetAtt, Join, Output

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
        temp_cmd = ['sudo', '/usr/bin/spark-submit', python_file] # creates 'python foo.py'
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

class EmrStepBuilder:

    def getStep(self, context, module_id, definition, config_file_location, workflow_version_id, workflow_module_id, deployment_yaml):
        
        t = Template()
        t.set_version("2010-09-09")
        _deployment_ = {}
        t.add_parameter(Parameter("RunId",Type="String"))
        deployment= definition['deployment']
        _deployment_["config"] = deployment
        _deployment_["type"] = "function"
        _deployment_["name"] = definition["metaData"]["name"]
        _deployment_["description"] = definition["metaData"]["description"]
        _deployment_["moduleid"] = module_id

        cft = build_emr_stack(_deployment_,context,t)
        
        if cft.to_dict()['Resources']:
            cft_definition = cft.to_yaml()
        
        encoded = base64.b64encode(cft_definition.encode('ascii')).decode('ascii')
        resource = {}
        resource["create_resources_function"]=constants.CREATE_RESOURCE
        resource["cft_template"]=encoded
        resource["delete_resources_step"]=constants.DELETE_RESOURCE 
        
        python_file, inputs, outputs, process = decode_yaml(config_file_location,module_id)
        cmds = cmds_creator(python_file, inputs, outputs, process)
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
        state_id = metadata['description']
        print("Emr", state_id)
        config = definition.get('deployment').get('config')
        jobtype = config.get('jobtype')
        stepName = "emrstep-" + time.strftime("%Y%m%d-%H:%M")
        outputs_dest = definition.get('output').get('dest')
        codeLocation    = utils.get_artifact_location(module_id)+"/"+definition['general']['scriptFile']
        # params = {"inputpyfile": codeLocation, "jobtype":jobtype, "input": inputs, "output": {"dest": outputs_dest}}
        params = {"resource_cft": resource,"inputpyfile": codeLocation,"jobtype":jobtype,"command": cmds,"dest": outputs_dest,"extra":extra}
        step = EmrAddStepStep(
            state_id=state_id,
            parameters={
                "stepName.$": stepName,
                "Payload": params
            }
        )
        return step