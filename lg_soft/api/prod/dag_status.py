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
        print("dag_status of dag_id "+dag_idd+" : '{}' ".format(i.get("state")))