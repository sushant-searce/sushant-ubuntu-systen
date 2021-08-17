import os
import time
import yaml
import boto3
import uuid
import flask
import pdb
import json
from urllib.request import urlopen
from itertools import chain
import ast
import constants

airflow_endpoint = constants.AIRFLOW_ENDPOINT
BUCKET_NAME = constants.SC_S3_BUCKET

#final = {}
#task_json = {}
task = []
final_task = []

def task_status(task_id, task_type, next_state_name, states):

    task_json = {}
    if next_state_name==None or next_state_name=="None":
        task_json['task_id'] = task_id
        task_json['task_type'] = task_type
        if next_state_name=="None":
            next_state_name = "end"
            task_json['Next'] = next_state_name.split("//")
        elif next_state_name==None:
            if "Deleting Resources" in states:
                next_state_name = "Deleting_Resources"
                task_json['Next'] = next_state_name.split("//")
            else:
                next_state_name = "end"
                task_json['Next'] = next_state_name.split("//")
        else:
            try:
                task_json['Next'] = next_state_name.split("//")
            except:
                task_json['Next'] = next_state_name
        task.append(task_json.copy())
        
    elif "parallelState" in next_state_name:
        
        try:
            task_json['task_id'] = task_id
            task_json['task_type'] = task_type
            branch1=[]
            parallelState = states[next_state_name]["Branches"]
            for branch in parallelState:
                branch1.append(branch["StartAt"])
            task_json['Next'] = branch1
            task.append(task_json.copy())
        except:
            pass
    
    elif "Choice" in next_state_name:
      
        choice_type = states[next_state_name]["Choices"][0]["Variable"]
        if choice_type=="${StatusCode}":
            try:
                task_json['task_id'] = task_id
                task_json['task_type'] = task_type
                branch1=[]
                choiceState = states[next_state_name]["Choices"]
                for choice in choiceState:
                    branch1.append(choice["Next"])
                task_json['Next'] = branch1
                task.append(task_json.copy())
            except:
                pass

        else:
            task_json['task_id'] = task_id
            task_json['task_type'] = task_type
            task_json['Next'] = next_state_name.split("//")
            task.append(task_json.copy())
    
    elif  "Choice" in task_id:
        task_json['task_id'] = task_id
        task_json['task_type'] = task_type
        next_state_name = ast.literal_eval(next_state_name)
        task_json['Next'] = next_state_name
        task.append(task_json.copy())

    else:
        task_json['task_id'] = task_id
        task_json['task_type'] = task_type
        if next_state_name=="None":
            next_state_name = "end"
            task_json['Next'] = next_state_name.split("//")
        elif next_state_name==None:
            next_state_name = "Deleting Resources"
            task_json['Next'] = next_state_name.split("//")
        elif next_state_name=="end" and task_id != "Deleting Resources":
            if "Deleting Resources" in states:
                next_state_name = "Deleting Resources"
                task_json['Next'] = next_state_name.split("//")
            else:
                next_state_name = "end"
                task_json['Next'] = next_state_name.split("//")
        else:
            try:
                task_json['Next'] = next_state_name.split("//")
            except:
                task_json['Next'] = next_state_name
        task.append(task_json.copy())
    
    #print(task)
    for i in task:
        if i not in final_task:
            final_task.append(i)

    return final_task


def create_dynamic_module(task_type, task_id, resource_type, states, next_state_name):
    
    _state_name = task_id.replace("_"," ")
    if task_type == "Task":
        if resource_type == 'arn:aws:states:::lambda:invoke':
            if task_id == 'Creating_Resources':
                dynamicTask = task_status('{}'.format(task_id), task_type, '{}'.format(next_state_name), states)
            elif task_id == 'Get_Resource_Creation_Status':
                dynamicTask = task_status('{}'.format(task_id), task_type, '{}'.format(next_state_name), states)
            elif task_id == 'Deleting_Resources':
                dynamicTask = task_status('{}'.format(task_id), task_type, '{}'.format(next_state_name), states)
            else:
                dynamicTask = task_status(task_id, task_type, next_state_name,states)
        elif resource_type == 'arn:aws:states:::ecs:runTask.sync':
            dynamicTask = task_status(task_id, task_type, next_state_name, states)
        elif resource_type == 'arn:aws:states:::sagemaker:createTransformJob.sync':
            dynamicTask = task_status(task_id, task_type, next_state_name, states)
        elif resource_type == 'arn:aws:states:::elasticmapreduce:addStep.sync':
            dynamicTask = task_status(task_id, task_type, next_state_name, states)
        elif resource_type == 'arn:aws:states:::glue:startJobRun.sync':
            dynamicTask = task_status(task_id, task_type, next_state_name, states)
    else:
        dynamicTask = task_status(task_id, task_type,next_state_name, states)
    return (dynamicTask)

def create_dynamic_task(state_name, states, upstream_task, downstream_task):
    states_length=len(states)
    for _state_name in states:
        if _state_name == state_name:
            state = states.get(_state_name)
            task_id = _state_name #.replace(' ', '_')
            break
    if state_name == None:
        return
    resource_type = state.get('Resource')
    task_type = state.get('Type')
    next_state_name = state.get('Next') or downstream_task
    end_state = state.get('End')
    if task_type == 'Parallel':
        if end_state == True:
            print("parallel state end is true")
            pass
        else:
            downstream_task = create_dynamic_task(next_state_name, states, None, downstream_task)
        
        branches = state.get('Branches')
        for branch in branches:
            downstream_task=next_state_name
            create_dynamic_branch(branch, upstream_task, downstream_task)

    elif task_type == 'Choice':
        if end_state == True:
            print("choice state end is true")
            pass
        else:
            # state_name
            next_state_name = state.get('Default')
            nxt_state_name = state.get('Default')
            if next_state_name!=None:
                downstream_task = create_dynamic_task(next_state_name, states, None, downstream_task)
            choices = state.get('Choices')
            choice_args = {}
            conditions = []
            choice_tasks = []
            is_custom_code = False

            for choice in choices:
                variable = choice.get("Variable")
                expression = list(choice)[1]
                expression_value = choice.get(expression)
                next_state_name = choice.get('Next')
                if 'StatusCode' in variable:
                    downstream_task = nxt_state_name
                    if expression_value == 200:
                        choice_task = create_dynamic_task(next_state_name, states, upstream_task, downstream_task)
                    else:
                        choice_task = create_dynamic_task(next_state_name, states, upstream_task, downstream_task)

                else:
                    is_custom_code = True
                    choice_task = create_dynamic_task(next_state_name, states, None, downstream_task)
            if (is_custom_code):
                nx = []
                for choice in choices:
                    nx.append(choice["Next"])
                downstream_task = str(nx)
                create_dynamic_module("choice", state_name, resource_type, states, downstream_task)

    else:
        _dynamic_task = create_dynamic_module(task_type, task_id, resource_type, states, next_state_name)
        upstream_task = _dynamic_task
        if end_state == True:
            upstream_task = downstream_task
        else:
            create_dynamic_task(next_state_name, states, upstream_task, downstream_task)
        return upstream_task

def create_dynamic_branch(branch, upstream_task, downstream_task):
    start = branch.get('StartAt')
    states = branch.get('States')
    create_dynamic_task(start, states, upstream_task, downstream_task)

def Taskstatus(workflow_version_id, workflow_execution_id):

    final = {}
    try:
        file_name= 'public/airflow/definationyaml/'+str(workflow_execution_id)+'/definition.yaml'
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_name)
        deployment = yaml.safe_load(response["Body"])
        updated_definition = deployment

        start = updated_definition.get('StartAt')
        dag_idd = "LG-dynamic-"+str(workflow_execution_id)
        s3_path = "public/airflow/Airflow-logs/"+dag_idd+"/"
        url = 'http://{}:8080/api/experimental/dags/{}/dag_runs'.format(airflow_endpoint,dag_idd)
        try:
            dags_status = urlopen(url).read().decode("utf-8")
            #print(dags_status)
        except:
            dag_status = "success"
        if "error" in dags_status:
            final['Dag_status'] = "initializing "
            final['Task_list'] = []
            obj = json.dumps(final, indent = 4)
        else:
            dags_status_json = json.loads(dags_status)
            for j in dags_status_json:
                running_dag_exe_date = j.get('execution_date')
                current_dag_status = j.get('state')

    except:
        return "Workflow failed please check your config files...."

    create_dynamic_branch(updated_definition, "start", "end")
    
    
    for i in final_task:
        state_name = i["task_id"]
        state_name = state_name.replace(" ", "_")
        try:
            url = 'http://{}:8080/api/experimental/dags/{}/dag_runs/{}/tasks/{}'.format(airflow_endpoint, dag_idd, running_dag_exe_date, state_name)
            task_state = urlopen(url).read().decode("utf-8")
            task_state_json = json.loads(task_state)
        except:
            pass
        task_statee = task_state_json.get('state')
        i["task_status"] = task_statee

    
    startjson = {}
    endjson = {}
    startjson["task_id"] = "start"
    startjson["task_type"] = "dummy_task"
    # startjson["task_status"] = "success"
    startjson["Next"] = start.split("/")
    endjson["task_id"] = "end"
    endjson["task_type"] = "dummy_task"
    # endjson["task_status"] = "success"
    endjson["Next"] = []
    
    index = len(final_task)
    final_task.insert(index,endjson)
    final_task.insert(0,startjson)

    
    final['Dag_status'] = current_dag_status 
    final['Task_list'] = final_task
    obj = json.dumps(final, indent = 4)
    #print(obj)
    return obj



