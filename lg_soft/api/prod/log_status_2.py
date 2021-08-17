import os, json, boto3, sys
import yaml

dag_id = "LG-dynamic-1608815739906"
task_id = "Splits_text_into_smaller_units"
s3_bucket_name = "airflow-logs-buckett"
s3_path = "Airflow-logs/"+dag_id+"/"

dags_status = os.popen('curl --silent http:/52.220.145.80:8080/api/experimental/dags/{}/dag_runs'.format(dag_id)).read()
dags_status_json = json.loads(dags_status)

list= []
for i in dags_status_json:
    list.append(i.get('execution_date'))

latest_execution=max(list)

s3_log_path = s3_path+task_id+"/"+latest_execution+"/1.log"
s3_client = boto3.client('s3')
response = s3_client.get_object(Bucket="airflow-logs-buckett", Key=s3_log_path)
filedata = response['Body'].read()
contents = filedata.decode('utf-8')

print(contents)
