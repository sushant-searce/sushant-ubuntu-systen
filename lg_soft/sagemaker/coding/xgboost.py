import yaml
import json
import boto3
import uuid
import base64
import time
import sagemaker
from sagemaker.tensorflow import TensorFlow
from sagemaker.tensorflow import TensorFlowModel
from sagemaker.xgboost.estimator import XGBoost
from sagemaker.xgboost.model import XGBoostModel
from sagemaker.pytorch.model import PyTorchModel
from sagemaker.local import LocalSession
from sagemaker.session import Session

s3client = boto3.client('s3')
region = "ap-southeast-1" 
model_s3_location = "s3://lppdatasource103424-devthree/public/modules/1616745187/Scripts/v1/xgboost/model.tar.gz"
inference_code = "inference.py"
role = "arn:aws:iam::797237262327:role/sagemaker-modules-sagemakerexecutionrole0D136CE5-120GAFR3HTKCW"
# default_bucket = "lppdatasource103424-devthree"
# boto_session = boto3.Session()
# sagemaker_client = boto_session.client(service_name='sagemaker', region_name=region)
# featurestore_runtime = boto_session.client(service_name='sagemaker-featurestore-runtime', region_name=region)
# sagemaker_session = sagemaker.Session(boto_session=boto_session,sagemaker_client=sagemaker_client,sagemaker_featurestore_runtime_client=featurestore_runtime)

xgboost_model = XGBoostModel(model_data=model_s3_location, role=role,entry_point=inference_code, framework_version="1.0-1")


#predictor = xgboost_model.deploy(initial_instance_count=1,instance_type='ml.t2.medium')
transformer = xgboost_model.transformer(instance_count=1, instance_type='ml.m4.xlarge')
transformer.transform('s3://lppdatasource103424-devthree/batch_data_cleansed.csv')
#predictor = xgboost_model.deploy(instance_type='ml.p2.xlarge',initial_instance_count=1,accelerator_type='ml.eia2.medium')
# ,sagemaker_session=sagemaker_session)
