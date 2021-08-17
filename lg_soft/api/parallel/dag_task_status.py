import yaml
import json
import os, json, boto3, sys

airflow_endpoint="52.220.145.80" 
dag_idd = "LG-dynamic-1612323219063" #"Emr-dynamic-1608755108597"
s3_bucket_name = "airflow-logs-buckett"
final = {}
task = []
steps_list=[]

def read_deployment_yaml():
    file_name= "1612323219063/definition.yaml"
    bucket = 'workflow-engine-data-sg'
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket, Key=file_name)
    deployment = yaml.safe_load(response["Body"])
    return deployment

updated_definition = read_deployment_yaml()

start = updated_definition.get('StartAt')
states = updated_definition.get('States')

task_list = updated_definition.get('States')
running_dags = os.popen('curl --silent http:/{}:8080/api/experimental/dags/{}/dag_runs'.format(airflow_endpoint,dag_idd)).read()
running_dags_json = json.loads(running_dags)

for j in running_dags_json:
    running_dag_exe_date = j.get('execution_date')
    current_dag_status = j.get('state')

for i in task_list:
    steps_list.append(i)

index = len(steps_list)
steps_list.insert(index,'end')
steps_list.insert(0,'start')
deployment_steps = {}

for j in task_list:
    i = j.replace(' ', '_')
    task_state =  os.popen('curl --silent http://{}:8080/api/experimental/dags/{}/dag_runs/{}/tasks/{}'.format(airflow_endpoint, dag_idd, running_dag_exe_date,i)).read()
    task_state_json = json.loads(task_state)
    task_statee = task_state_json.get('state')
    deployment_steps['task_id']= i
    deployment_steps['task_status']= task_statee
    
    if states[j]['Type']=='Parallel':
        nxt = []
        nxt.append(task_list.get(j).get('Next'))
        deployment_steps['Next']= nxt

        x = task_list.get(j).get('Branches')
        branches={}
        branch = []
        for i in x:
            print(x)
            # next.append(i.get('StartAt'))
            # pdict={}
            # for j in next:
            #     task_state =  os.popen('curl --silent http://{}:8080/api/experimental/dags/{}/dag_runs/{}/tasks/{}'.format(airflow_endpoint, dag_idd, running_dag_exe_date,j)).read()
            #     task_state_json = json.loads(task_state)
            #     task_statee = task_state_json.get('state')
            #     pdict['task_id'] = j
            #     deployment_steps['testpara']= pdict
        
            # task.append(deployment_steps.copy())
        
    else:
        tp = []
        tp.append(task_list.get(j).get('Next'))
        deployment_steps['Next']= tp
        task.append(deployment_steps.copy())

final['Dag_status'] = current_dag_status 
final['Task_list'] = task
obj = json.dumps(final, indent = 4)
# print(obj)
