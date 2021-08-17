import os, json, boto3, sys, yaml

dag_idd = "LG-dynamic-1608883746438"

dags_status = os.popen('curl --silent http:/52.220.145.80:8080/api/experimental/dags/{}/dag_runs'.format(dag_idd)).read()
dags_status_json = json.loads(dags_status)

list= []
for i in dags_status_json:
    list.append(i.get('execution_date'))

latest_execution=max(list)

for i in dags_status_json:
    if i.get('execution_date')==latest_execution:
        dag_state = i.get("state")

final = {}
task = []
steps_list=[]
deployment_steps = {}

def read_deployment_yaml():
    file_name= "1608883746438/definition.yaml"
    bucket = 'workflow-engine-data-sg'
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket, Key=file_name)
    deployment = yaml.safe_load(response["Body"])
    return deployment

updated_definition = json.loads(read_deployment_yaml())
states = updated_definition.get('States')

for i in states:
    steps_list.append(i)

index = len(steps_list)
steps_list.insert(index,'end')
steps_list.insert(0,'start')

for j in states:
    i = j.replace(' ', '_')
    task_state =  os.popen('curl --silent http://52.220.145.80:8080/api/experimental/dags/{}/dag_runs/{}/tasks/{}'.format(dag_idd, latest_execution,i)).read()
    task_state_json = json.loads(task_state)
    task_statee = task_state_json.get('state')
    deployment_steps['task_id']= i
    deployment_steps['task_status']= task_statee
    nx = []
    nx.append(states.get(j).get('Next'))
    deployment_steps['Next']= nx
    task.append(deployment_steps.copy())

final['Dag_status'] = dag_state 
final['Task_list'] = task
obj = json.dumps(final, indent = 4)
print(obj)
