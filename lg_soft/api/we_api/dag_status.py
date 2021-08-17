@app.route('/workflow/<workflow_version_id>/execution/<workflow_execution_id>/airflowstatus', endpoint="dag_execution_status",
           methods=['GET'])
def dag_execution_status(workflow_version_id, workflow_execution_id):

    final = {}
    dag_idd = "LG-dynamic-"+str(workflow_execution_id)
    dags_status = os.popen('curl --silent http://{}:8080/api/experimental/dags/{}/dag_runs'.format(airflow_endpoint,dag_idd)).read()
    dags_status_json = json.loads(dags_status)
    list= []
    for i in dags_status_json:
        list.append(i.get('execution_date'))

    latest_execution=max(list)

    for i in dags_status_json:
        if i.get('execution_date')==latest_execution:
            dag_state = i.get("state")
            final['Dag_status'] = dag_state

    obj = json.dumps(final, indent = 4)
    return obj