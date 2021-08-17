from datetime import datetime
import yaml
from troposphere.awslambda import Function, Code, Tags, Content
from troposphere.ecs import TaskDefinition
from troposphere.iam import Policy, Role
from troposphere import Parameter, Ref, Template, GetAtt, Join, Output
from troposphere.sagemaker import Model, ContainerDefinition
from sagemaker.amazon.amazon_estimator import get_image_uri
from troposphere import Parameter, Ref, Template, Tags, If, Equals, Not, Join, Output
from troposphere.awslambda import Version
from troposphere.constants import C1_MEDIUM, KEY_PAIR_NAME, M1_LARGE, M1_MEDIUM, SUBNET_ID, M4_LARGE, NUMBER
import troposphere.emr as emr 
from troposphere.emr import Application, Step
import troposphere.iam as iam
from troposphere import ( AWS_ACCOUNT_ID, Parameter, Ref, Template, Tags, If, Equals, Not, Join, Output)
from troposphere import glue
from troposphere.iam import ManagedPolicy, Policy, Role
from troposphere.glue import ( CatalogTarget, Crawler, Database, DatabaseInput, Job, JobCommand, S3Target, Schedule, SchemaChangePolicy, Targets)
from constants import SAGEMAKER_EXECUTION_ARN
import uuid
import constants
#import os,sys,inspect
#current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
#parent_dir = os.path.dirname(current_dir)
#sys.path.insert(0, parent_dir) 
import utils
#import constants
from zipfile import ZipFile
from os.path import basename
import os
import boto3
import subprocess
import troposphere.ec2 as ec2
from troposphere.iam import InstanceProfile, Role
from troposphere import Base64
import random
import string
import json

#sagemaker_execution_arn = SAGEMAKER_EXECUTION_ARN

def build_lambda_stack(definition,context, zip_location,t):
    outputs = []
    bucket_name = zip_location.get('bucket')
    key_name=zip_location.get('object_name')
    #layer_zip_name = layer_zip_location.get('object_name')
    layer_name='testlayer'+str(uuid.uuid4())[:8]
    # TODO: Need to build the execution policy in a fine grained manner.
    #  Right now, we are adding a role to the lambda that gives the lambda big permission
    rolename = "LambdaExecutionRole"+str(uuid.uuid4())[:8]
    fun_name=definition.get("description")
    function_name=fun_name.replace(" ", "") 
    t.add_resource(Role(
        rolename,
        Path="/",
        Policies=[Policy(
            PolicyName="root",
            PolicyDocument={
                "Version": "2012-10-17",
                "Statement": [{
                    "Action": ["logs:*"],
                    "Resource": "arn:aws:logs:*:*:*",
                    "Effect": "Allow"
                }, {
                    "Action": "*",
                    "Resource": "*",
                    "Effect": "Allow"
                }]
            })],
        AssumeRolePolicyDocument={"Version": "2012-10-17", "Statement": [
            {
                "Action": ["sts:AssumeRole"],
                "Effect": "Allow",
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]},
    ))

    import constants

    
    function = t.add_resource(Function(
        function_name,
        Code=Code(
            S3Bucket=bucket_name,
            S3Key=key_name
        ),
        Handler=definition.get("config").get("config").get("handler"),
        FunctionName=Join('', ['lambda-', definition.get("config").get("config").get("name"), "-", Ref('RunId'),"-",str(uuid.uuid4())[:8]]),
        Role=GetAtt(rolename, "Arn"),
        Runtime=definition.get("config").get("config").get("runTime"),
        MemorySize=definition.get("config").get("config").get("MemorySize"),
        Timeout=900,
        Tags=Tags(user = context['tags']['user'], 
                  team = context['tags']['team'],
                  department = context['tags']['department'],
                  executionId = context['tags']['executionId']
        )  
                

    ))

    _lambda = Output(function_name, Description="Output from Lambda Step", Value=Ref(function), )
    outputs.append(_lambda)
    t.add_output(outputs)
    return t

def build_model_stack(definition, t):
    #model_name = definition.get("name") + datetime.now().strftime("%Y%m%d%H%M%S")
    container_definition = ContainerDefinition(
        ModelDataUrl=definition.get("config").get("ArtifactLocation"),
        Image=get_image_uri('ap-south-1', definition.get("config").get("image")
                            , repo_version='latest'))
    model = t.add_resource(Model("SagemakerModel",
                                 ExecutionRoleArn=sagemaker_execution_arn,
                                 ModelName=Join('',
                                                ['model-', definition.get("name"), "-", Ref('RunId')]),
                                 Containers=[container_definition]))
    return Output(definition.get("name"), Description="Output From Model Step",
                  Value=GetAtt(model, "ModelName"))

def build_ecs_instance(definition, context,t):
    
    outputs = []
    ssmclient = boto3.client('ssm')
    json_string = ssmclient.get_parameter(Name='/aws/service/ecs/optimized-ami/amazon-linux-2/recommended')['Parameter']['Value']
    AMI = json.loads(json_string)['image_id']
    ec2_name=definition.get("description")
    ec2_instance_name=ec2_name.replace(" ", "") 
    # xtr=random.randint(0,100)
    # xtr=str(xtr)
    res = ''.join(random.choices(string.ascii_uppercase, k = 4)) 
    ec2_res='EC2InstanceProfile'+str(res)
    image_id=definition.get("config").get("config").get("ImageId")
    instance_type=definition.get("config").get("config").get("InstanceType")
    EC2InstanceProfile = t.add_resource(InstanceProfile(
    ec2_res,
    Path='/',
    Roles=['ecsInstanceRole'],
    ))

    ec2_instance = t.add_resource(ec2.Instance(
        ec2_instance_name,
        ImageId=image_id,
        InstanceType=instance_type,
        IamInstanceProfile=Ref(ec2_res),
        SecurityGroups=["default"],
        UserData=Base64(Join('',
        [
            '#!/bin/bash',
            '\n',
            'sudo amazon-linux-extras disable docker',
            '\n',
            'sudo mkdir -p /etc/ecs && sudo touch /etc/ecs/ecs.config',
            '\n', 
            'echo ECS_CLUSTER=ecs-ec2 > /etc/ecs/ecs.config',
            '\n',
            'sudo amazon-linux-extras install -y ecs',
            '\n',
            'sudo systemctl enable --now --no-block ecs'
        ])),
        Tags=[{'Key':'user','Value':context['tags']['user']},
              {'Key':'team','Value': context['tags']['team']},
              {'Key':'department','Value':context['tags']['department']},
              {'Key':'executionId','Value': context['tags']['executionId']}]
    ))
    _ecs = Output(ec2_instance_name, Description="Output from ec2 Step", Value=Ref(ec2_instance))
    outputs.append(_ecs)
    t.add_output(outputs)
    return t
    #return Output(ec2_instance_name, Description="Output from ec2 Step",Value=Ref(ec2_instance))

def build_ecs_task(definition, t):
    task_name = definition.get("config").get("Name")
    cpu = definition.get("config").get("Cpu")
    memory = definition.get("config").get("Memory")
    image_name = definition.get("config").get("Image")
    task_definition = t.add_resource(TaskDefinition(
        task_name,
        RequiresCompatibilities=['FARGATE'],
        Cpu=cpu,
        Memory=memory,
        NetworkMode='awsvpc',
        ContainerDefinitions=[
            ContainerDefinition(
                Name='container1',
                Image='925881846319.dkr.ecr.ap-south-1.amazonaws.com/lpp/dockers:lpp_wrapper_mumbai',
                Essential=False,
                PortMappings = [
                    PortMapping(
                        ContainerPort=80,
                        HostPort=80,
                        Protocol='tcp'
                    )
                ],
                EntryPoint=[
                    "/tmp/home/start.sh"
                ],
                MountPoints=[
                    MountPoint(
                        SourceVolume='lpp',
                        ContainerPath='/tmp/home'
                    )
                ],
                LogConfiguration=[
                    LogConfiguration(
                        LogDriver='awslogs',
                        Options={
                            "awslogs-group": "/ecs/ecsTask",
                            "awslogs-region": "ap-south-1",
                            "awslogs-stream-prefix": "ecs"
                        }
                    )
                ]
            ),
            ContainerDefinition(
                Name='container2',
                Image=image_name,
                Essential=False,
                PortMappings = [
                    PortMapping(
                        ContainerPort=81,
                        HostPort=81,
                        Protocol='tcp'
                    )
                ],
                EntryPoint=[
                    "python"
                ],
                Command=[
                    "yaml_parser.py",
                    "--yaml",
                    "tempconfig.yaml"
                ],
                MountPoints=[
                    MountPoint(
                        SourceVolume='lpp',
                        ContainerPath='/tmp/home'
                    )
                ],
                DependsOn=[
                    ContainerDependency(
                        Condition="COMPLETE",
                        ContainerName="container1"
                    )
                ],
                LogConfiguration=[
                    LogConfiguration(
                        LogDriver='awslogs',
                        Options={
                            "awslogs-group": "/ecs/ecsTask",
                            "awslogs-region": "ap-south-1",
                            "awslogs-stream-prefix": "ecs"
                        }
                    )
                ]
            ),
            ContainerDefinition(
                Name='container3',
                Image='925881846319.dkr.ecr.ap-south-1.amazonaws.com/lpp/dockers:lpp_wrapper_mumbai',
                Essential=True,
                PortMappings = [
                    PortMapping(
                        ContainerPort=82,
                        HostPort=82,
                        Protocol='tcp'
                    )
                ],
                EntryPoint=[
                    "/tmp/home/end.sh"
                ],
                MountPoints=[
                    MountPoint(
                        SourceVolume='lpp',
                        ContainerPath='/tmp/home'
                    )
                ],
                DependsOn=[
                    ContainerDependency(
                        Condition="COMPLETE",
                        ContainerName="container2"
                    )
                ],
                LogConfiguration=[
                    LogConfiguration(
                        LogDriver='awslogs',
                        Options={
                            "awslogs-group": "/ecs/ecsTask",
                            "awslogs-region": "ap-south-1",
                            "awslogs-stream-prefix": "ecs"
                        }
                    )
                ]
            )
        ]
    ))
    t.add_resource(task_definition)
    return Output(task_name, Description="Output From ECS Create Task Step",
                  Value=Ref(task_definition))

def build_emr_stack(definition,context,t):
    
    outputs = []
    uid = uuid.uuid4().hex[:4]
    emr_service_role = t.add_resource(iam.Role(
        'EMRServiceRole',
        RoleName= 'lg-dev-emrServiceRole-'+uid,
        AssumeRolePolicyDocument={
            "Version": "2008-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {
                    "Service": [
                        "elasticmapreduce.amazonaws.com"
                    ]
                },
                "Action": ["sts:AssumeRole"]
            }]
        },               
        ManagedPolicyArns=[
            'arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole',
            'arn:aws:iam::aws:policy/AmazonS3FullAccess'
        ]
    ))
    # EMR EC2 Role
    emr_job_flow_role = t.add_resource(iam.Role(
        "EMRJobFlowRole",
        RoleName= 'lg-dev-emrEc2Role-'+uid,
        AssumeRolePolicyDocument={
            "Version": "2008-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {
                    "Service": [
                        "ec2.amazonaws.com"
                    ]
                },
                "Action": ["sts:AssumeRole"]
            }]
        },
        ManagedPolicyArns=[
            'arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role',
            'arn:aws:iam::aws:policy/AmazonS3FullAccess'
        ]
    ))
    emr_instance_profile = t.add_resource(iam.InstanceProfile(
        "EMRInstanceProfile",
        Roles=[Ref(emr_job_flow_role)]
    ))
    ###########################################
    ## Dynamically fetch the required values ##
    ###########################################
    clusterName = definition.get("config").get("config").get("clusterName") 
    releaseLabel = definition.get("config").get("config").get("releaseLabel")
    logUri = "s3://"+constants.SC_S3_BUCKET+"/public/airflow/elasticmapreduce/"
    ec2KeyPair = constants.EC2KEYPAIR
    ec2SubnetId = constants.FARGATE_SUBNET
    #Master instance details
    masterInstanceName = definition.get("config").get("config").get("masterInstanceName")
    masterInstanceCount = definition.get("config").get("config").get("masterInstanceCount")
    masterInstanceType = definition.get("config").get("config").get("masterInstanceType")
    masterInstanceMarket = definition.get("config").get("config").get("masterInstanceMarket")
    #Core instance details
    coreInstanceName = definition.get("config").get("config").get("coreInstanceName")
    coreInstanceCount = definition.get("config").get("config").get("coreInstanceCount")
    coreInstanceType = definition.get("config").get("config").get("coreInstanceType")
    coreInstanceMarket = definition.get("config").get("config").get("coreInstanceMarket")
    #Task instance details
    taskInstanceName = definition.get("config").get("config").get("taskInstanceName")
    taskInstanceCount = definition.get("config").get("config").get("taskInstanceCount")
    taskInstanceType = definition.get("config").get("config").get("taskInstanceType")
    taskInstanceMarket = definition.get("config").get("config").get("taskInstanceMarket")
    # Application Type (Spark ,Hadoop, Hive, Mahout, Pig)
    applicationName = definition.get("config").get("config").get("applicationName")
    user = context['tags']['user']

    ####################
    # #Cluster Details ##
    ####################
    cluster = t.add_resource(emr.Cluster(
        'EMRCluster',
        Name=clusterName,
        ReleaseLabel=releaseLabel,
        JobFlowRole=Ref(emr_instance_profile),
        ServiceRole=Ref(emr_service_role),
        Tags=[{'Key':'user','Value':context['tags']['user']},
              {'Key':'team','Value': context['tags']['team']},
              {'Key':'department','Value':context['tags']['department']},
              {'Key':'executionId','Value': context['tags']['executionId']}],      
        VisibleToAllUsers=True,
        Instances=emr.JobFlowInstancesConfig(
            Ec2KeyName=ec2KeyPair,
            Ec2SubnetId=ec2SubnetId, 
            MasterInstanceGroup=emr.InstanceGroupConfigProperty(
                Name=masterInstanceName,
                InstanceCount=masterInstanceCount,
                InstanceType=masterInstanceType,
                Market=masterInstanceMarket # EC2 Market : 'SPOT' or 'ON_DEMAND'
            ),
            CoreInstanceGroup=emr.InstanceGroupConfigProperty(
                Name=coreInstanceName,
                Market=coreInstanceMarket,  # EC2 Market :- 'SPOT' or 'ON_DEMAND'
                InstanceCount=coreInstanceCount,
                InstanceType=coreInstanceType,
            )
        ),
        Applications=[
            emr.Application(Name=applicationName)
        ],
    ))

    tasknodes = t.add_resource(emr.InstanceGroupConfig(
        'TaskNodes',
        Name=taskInstanceName,
        InstanceCount=taskInstanceCount,
        InstanceRole='TASK',
        InstanceType=taskInstanceType,
        JobFlowId=Ref(cluster),
        Market=taskInstanceMarket
    ))
    
    _emr = Output(clusterName, Value=Ref(cluster), Description='Output of EMR Cluster')
    outputs.append(_emr)
    t.add_output(outputs)
    return t


# def build_cft_definition(config_yaml, context):
#     t = Template()
#     t.set_version("2010-09-09")
#     outputs = []
#     t.add_parameter(Parameter("RunId",Type="String"))
#     deployments = config_yaml['deployments']
#     for deployment in deployments:
#         deployment_type = deployment.get('type')
#         if deployment_type == "function":
#             zip_location = zip_s3_files(deployment.get('moduleid'))
#             outputs.append(build_lambda_stack(deployment,context,zip_location, t))
#             t.add_output(outputs)
#             return t
#         elif deployment_type == "model":
#             outputs.append(build_model_stack(deployment, t))
#             t.add_output(outputs)
#             return t
#         elif deployment_type == "emr":
#             outputs.append(build_emr_stack(deployment,context,t))
#             t.add_output(outputs)
#             return t
#         elif deployment_type == "ecs":
#             pass
#         elif deployment_type == "ecs-ec2":
#             TeamCluster = True
#             if not TeamCluster:
#                 outputs.append(build_ecs_instance(deployment, context, t))
#                 t.add_output(outputs)
#             else:
#                 pass

