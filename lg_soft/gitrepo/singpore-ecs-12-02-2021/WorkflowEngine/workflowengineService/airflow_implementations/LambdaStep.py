from stepfunctions.steps import *
import utils
import constants
import boto3
import uuid
import base64
from zipfile import ZipFile
from os.path import basename
import os,sys,inspect
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
sys.path.insert(0, current_dir) 
from Assemble import build_lambda_stack
from troposphere import Parameter, Ref, Template, GetAtt, Join, Output


def fetch_config_from_db(config_id, type):
    return utils.fetch_config_from_db(config_id, type)

def zip_s3_files(moduleid):
    artifact_location=utils.get_artifact_location(moduleid)
    s3_client = boto3.resource('s3')
    if artifact_location.startswith('s3://'):
        artifact_location = artifact_location[5:]
        s3_components = artifact_location.split('/')
        bucket_name = s3_components[0]
        key=""
        if len(s3_components)>1:
            key='/'.join(s3_components[1:])

    cmd="mkdir -p /tmp/scripts/"
    os.system(cmd)
    bucket = s3_client.Bucket(bucket_name)
    x=os.listdir('/tmp/')
    folder=artifact_location.split('/')[-1]
    path="/tmp/scripts/"+folder
    cmd="mkdir -p " + path
    os.system(cmd)
    for obj in bucket.objects.filter(Prefix = key):
        name=obj.key.split('/')[-1]
        new_path=path+"/"+name
        bucket.download_file(obj.key, new_path)
    x=os.listdir(path)
    for file in x:
        pass
    zip_path = "/tmp/scripts/" + folder +"-scripts"+ str(uuid.uuid4())[:8]+".zip"
    with ZipFile(zip_path, 'w') as zipObj:
        for folderName, subfolders, filenames in os.walk(path):
            for filename in filenames:
                filePath = os.path.join(folderName, filename)
                zipObj.write(filePath, basename(filePath))
    s3_client = boto3.client('s3')
    file_name = zip_path
    bucket = bucket_name
    local_file_name = zip_path.split('/')[-1]
    object_name="lambda_zip_scripts/"+local_file_name
    response = s3_client.upload_file(file_name, bucket, object_name)
    cmd="rm " + zip_path
    os.system(cmd)
    cmd="rm -rf " + path
    os.system(cmd)
    return{
        'bucket': bucket,
        'object_name' : object_name
    }

class LambdaStepBuilder:


    def getStep(self, context, module_id,definition, config_file_location, workflow_version_id, workflow_module_id, deployment_yaml):
        
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
        zip_location = zip_s3_files(module_id)

        cft = build_lambda_stack(_deployment_,context,zip_location,t)
        
        if cft.to_dict()['Resources']:
            cft_definition = cft.to_yaml()
        
        encoded = base64.b64encode(cft_definition.encode('ascii')).decode('ascii')
        resource = {}
        resource["create_resources_function"]=constants.CREATE_RESOURCE
        resource["cft_template"]=encoded
        resource["delete_resources_step"]=constants.DELETE_RESOURCE
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
        print("LambdaLoader", state_id)
        artifact_location = utils.get_artifact_location(module_id)
        print("artifact_location",artifact_location)
        function_name = utils.resolvePlaceHolderValues(metadata['description'], context)
        inputs = definition['input']['source']
        functions = definition['functional']['process']
        
        outputs_dest = definition['output']['dest']
        params = {"resource_cft": resource ,"input": {"source": inputs}, "functional": {"process":functions}, "output": {"dest": outputs_dest},"extra":extra,"config_file_location":config_file_location,"artifact_location":artifact_location}
        step = LambdaStep(
            state_id=state_id,
            parameters={
                "FunctionName.$": function_name,
                "Payload": params
            }
        )
        return step
