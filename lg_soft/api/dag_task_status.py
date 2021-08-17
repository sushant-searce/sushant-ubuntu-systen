import yaml
import json
import os, json, boto3, sys

dag_idd = "LG-dynamic-1609239466658" #"Emr-dynamic-1608755108597"
s3_bucket_name = "airflow-logs-buckett"
final = {}
task = []
steps_list=[]

def read_deployment_yaml():
    with open(r'/home/sushantnigudkar/Downloads/definitionv1.yaml') as file:
       deployment = yaml.load(file, Loader=yaml.FullLoader)
    return deployment

updated_definition = json.loads(read_deployment_yaml())
task_list = updated_definition.get('States')
running_dags = os.popen('curl --silent http:/52.220.145.80:8080/api/experimental/dags/{}/dag_runs'.format(dag_idd)).read()
running_dags_json = json.loads(running_dags)

for j in running_dags_json:
    if j.get('state')=='failed':
        a = j
        running_dag_exe_date = a.get('execution_date')

for i in task_list:
    steps_list.append(i)

index = len(steps_list)
steps_list.insert(index,'end')
steps_list.insert(0,'start')
deployment_steps = {}

for j in task_list:
    i = j.replace(' ', '_')
    task_state =  os.popen('curl --silent http://52.220.145.80:8080/api/experimental/dags/{}/dag_runs/{}/tasks/{}'.format(dag_idd, running_dag_exe_date,i)).read()
    task_state_json = json.loads(task_state)
    task_statee = task_state_json.get('state')
    deployment_steps['task_id']= i
    deployment_steps['task_status']= task_statee
    if i=='parallel':
        next = []
        x = task_list.get(j).get('Branches')
        for i in x:
            next.append(i.get('StartAt'))
            deployment_steps['Next']= next
            pdict={}
            for j in next:
                task_state =  os.popen('curl --silent http://52.220.145.80:8080/api/experimental/dags/{}/dag_runs/{}/tasks/{}'.format(dag_idd, running_dag_exe_date,j)).read()
                task_state_json = json.loads(task_state)
                task_statee = task_state_json.get('state')
                pdict['task_status']= task_statee
                pdict['task_id'] = j
                deployment_steps['testpara']= pdict
        
            task.append(deployment_steps.copy())
                
    else:
        tp = []
        tp.append(task_list.get(j).get('Next'))
        deployment_steps['Next']= tp
        task.append(deployment_steps.copy())

final['Dag_status'] = 'failed' 
final['Task_list'] = task
obj = json.dumps(final, indent = 4)
print(obj)
           
