import os, json, boto3, sys
import yaml

final = {}
deployment_steps = {}
task = []
steps_list=[]

dag_id = "LG-dynamic-1608815739906"
s3_bucket_name = "airflow-logs-buckett"
s3_path = "Airflow-logs/"+dag_id+"/"

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

dags_status = os.popen('curl --silent http:/52.220.145.80:8080/api/experimental/dags/{}/dag_runs'.format(dag_id)).read()
dags_status_json = json.loads(dags_status)

list= []
for i in dags_status_json:
    list.append(i.get('execution_date'))

latest_execution=max(list)

def read_deployment_yaml():
    file_name= "1608815739906/definition.yaml"
    bucket = 'workflow-engine-data-sg'
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket, Key=file_name)
    deployment = yaml.safe_load(response["Body"])
    return deployment


updated_definition = json.loads(read_deployment_yaml())
task_list = updated_definition.get('States')

for i in task_list:
    steps_list.append(i)

index = len(steps_list)
steps_list.insert(index,'end')
steps_list.insert(0,'start')
final = {}
deployment_steps = {}
task = []
for j in task_list:
    i = j.replace(' ', '_')
    s3_log_path = s3_path+i+"/"+latest_execution+"/1.log"
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket="airflow-logs-buckett", Key=s3_log_path)
    filedata = response['Body'].read()
    contents = filedata.decode('utf-8')
    deployment_steps['task_id'] = i
    deployment_steps['task_logs'] = contents
    task.append(deployment_steps.copy())

final['Task_list'] = task
obj = json.dumps(final, indent = 4)
print(obj)
