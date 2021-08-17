# TODO: Clean this up. Either make it come from Parameter Store or Implicitly use Role ARN for the ECS Task.
import os

ROLE = os.environ.get('ROLE')

WORKFLOW_EXEC_ROLE = os.environ.get('WORKFLOW_EXEC_ROLE')
DYNMODB_TABLE = os.environ.get('DYNMODB_TABLE')

NOTIFICATION_TOPIC = os.environ.get('NOTIFICATION_TOPIC')
FARGATE_CLUSTER =  os.environ.get('FARGATE_CLUSTER')
FARGATE_TASK_DEF = os.environ.get('FARGATE_TASK_DEF')
LPP_FARGATE_TASK_DEF = os.environ.get('LPP_FARGATE_TASK_DEF')
TASK_EXECUTION_ROLE = os.environ.get('TASK_EXECUTION_ROLE')
# STARTER_STOPPER = '925881846319.dkr.ecr.ap-northeast-1.amazonaws.com/lpp_modules_docker_repo:lpp_wrapper'
#TASK_EXECUTION_ROLE = os.environ.get('TASK_EXECUTION_ROLE')
STARTER_STOPPER = os.environ.get('STARTER_STOPPER')
FARGATE_SEC_GROUP =os.environ.get('FARGATE_SEC_GROUP')
FARGATE_SUBNET = os.environ.get('FARGATE_SUBNET')
FARGATE_CONTAINER_NAME =  "ECSModule"
# FINAL_STEP = os.environ.get('FINAL_STEP')
FINAL_STEP = "arn:aws:lambda:ap-southeast-1:797237262327:function:Project_Completion_Step"
LOG_GROUP = "/ecs/ecsTask"
# LOG_GROUP = os.environ.get('LOG_GROUP')
S3_BASE_BUCKET_NAME= os.environ.get('S3_BASE_BUCKET_NAME')
S3_BASE_LOCATION = "s3://"+S3_BASE_BUCKET_NAME
CREATE_RESOURCE = os.environ.get('CREATE_RESOURCE')
GET_CFT_STATUS =  os.environ.get('GET_CFT_STATUS')
DELETE_RESOURCE = os.environ.get('DELETE_RESOURCE')
SAGEMAKER_EXECUTION_ARN = os.environ.get('SAGEMAKER_EXECUTION_ARN')
SC_PORTFOLIO_ID = os.environ.get('SC_PORTFOLIO_ID')
SC_S3_BUCKET = os.environ.get('SC_S3_BUCKET')

LAYERS_NAME = os.environ.get('LAYERS_NAME')
MODEL_NAME = os.environ.get('MODEL_NAME')

MODULE_CONFIG_VERSION = "moduleConfigVersion"
WORKFLOW_CONFIG_VERSION = "workflowVersion"
WORKFLOW_EXECUTION_VERSION = "workflowExecution"
AIRFLOW_ENDPOINT = "ec2-52-220-145-80.ap-southeast-1.compute.amazonaws.com"
ORCHESTRATION_ENGINE = os.environ.get('ORCHESTRATION_ENGINE')
EC2KEYPAIR = 'searce-ap-southeast-1-kp' #os.environ.get('EC2-KEY-PAIR')

AIRFLOW_DAG_BUCKET_NAME = "dynamic-airflow-dags"
WORKFLOW_EXECUTION_YAML_BUCKET_NAME = "workflow-engine-data-sg"
AIRFLOW_LOGS_BUCKET = "airflow-logs-buckett"
