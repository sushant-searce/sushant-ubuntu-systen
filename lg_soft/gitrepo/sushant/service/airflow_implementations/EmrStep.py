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
import boto3
import os
import subprocess
from Assemble import build_emr_stack
from troposphere import Parameter, Ref, Template, GetAtt, Join, Output

def fetch_config_from_db(config_id, type):
    return utils.fetch_config_from_db(config_id, type)

def decode_yaml(data_loaded_config, module_id,context, codeLocation):
    data_loaded= yaml.safe_load(data_loaded_config)
    python_file = codeLocation #utils.get_artifact_location(module_id)+"/"+utils.resolvePlaceHolderValues(data_loaded['general']['scriptFile'],context)
    inputs = data_loaded['input']['source']
    if data_loaded.get('input').get('dictionary'):
        dicts = data_loaded['input']['dictionary']
        inputs.extend(dicts)
    outputs = data_loaded["output"]["dest"]
    process = data_loaded.get("functional").get("process")
    out = [python_file, inputs, outputs, process]
    return out

def cmds_creator(python_file, inputs, outputs, process, context,cmds = []):
    ips = []
    output = []
    # input_args = []
    # output_args = []
    if cmds == []:
        for i in inputs:
            ips.append([i["argName"], i["filePath"]])
        for j in outputs:
            output.append([j["argName"], j["filePath"]])
        temp_cmd = [python_file] # creates 'python foo.py'
        for i in range(len(ips)):
            temp_cmd.append('--{}'.format(ips[i][0]))
            temp_cmd.append(ips[i][1])
        for i in range(len(output)):
            temp_cmd.append('--{}'.format(output[i][0]))
            temp_cmd.append(output[i][1])
        if process:
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
    def getStep(self, context, module_config, workflow_version_id):
        module_id = module_config.get('platformModuleId')
        definition = module_config.get('config')
        config_string = module_config.get('config_string')
        module_name = module_config.get('module_name')
        workflow_module_id = module_config.get('module_id')
        
        s3_location = utils.get_artifact_location(module_id)+"/"+definition['general']['scriptFile']
        name_bucket = s3_location.split('/')[2]
        name_key = '/'.join(s3_location.split('/')[3:])    
        folder=os.path.split(name_key)[0]
        s3_client = boto3.resource('s3')
        bucket = s3_client.Bucket(name_bucket)
        path = "/mnt/dags/emr/{}/".format(context['tags']['executionId'])
        cmd="mkdir -p {}".format(path)
        out=subprocess.check_output(cmd,shell=True)
        for obj in bucket.objects.filter(Prefix=folder):
            newpath = path+"requirements.txt"
            if obj.key.endswith(".txt"):
                bucket.download_file(obj.key, newpath)
        
        mainfn="""#!/bin/bash -xe
#Non-standard and non-Amazon Machine Image Python modules:"""
        file_name = path+"pip.sh"
        open(file_name, "w").write(mainfn)

        with open(file_name, "a+") as file_object:
            file_object.seek(0)
            data = file_object.read(100)
            requirements = open(newpath, "r")
            for line in requirements:
                if line.startswith ("#"):
                    pass
                else:
                    file_object.write("sudo pip3 install " + line)
        
        object_name ='public/Workflow/ExecutionFiles/{0}/{1}/{2}'.format(workflow_version_id,context['tags']['executionId'],"requirements.sh")
        pat = file_name
        s3 = boto3.client('s3')
        nm_bucket=constants.SC_S3_BUCKET
        with open(pat, "rb") as f:
            s3.upload_fileobj(f,nm_bucket,object_name)
        print("script is uploaded to S3")
        
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

        cft = build_emr_stack(_deployment_,object_name,context,t)
        
        if cft.to_dict()['Resources']:
            cft_definition = cft.to_yaml()
        
        encoded = base64.b64encode(cft_definition.encode('ascii')).decode('ascii')
        resource = {}
        resource["create_resources_function"]=constants.CREATE_RESOURCE
        resource["cft_template"]=encoded
        resource["delete_resources_step"]=constants.DELETE_RESOURCE 
        
        codeLocation = utils.get_artifact_location(module_id)+"/"+utils.resolvePlaceHolderValues(definition['general']['scriptFile'],context)
        python_file, inputs, outputs, process = decode_yaml(config_string, module_id,context, codeLocation)
        cmds = []
        CMDS1 = cmds_creator(python_file, inputs, outputs, process,context,cmds)
        print('commands', CMDS1)

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
        state_id = module_name # metadata['description']
        print("Emr", state_id)
        config = definition.get('deployment').get('config')
        jobtype = config.get('jobtype')
        stepName = "emrstep-" + time.strftime("%Y%m%d-%H:%M")
        outputs_dest = definition.get('output').get('dest')
        params = {"resource_cft": resource, "inputpyfile": codeLocation, "jobtype":jobtype, "command": CMDS1, "dest": outputs_dest, "extra":extra}
        step = EmrAddStepStep(
            state_id=state_id,
            parameters={
                "stepName.$": stepName,
                "Payload": params
            }
        )
        return step
