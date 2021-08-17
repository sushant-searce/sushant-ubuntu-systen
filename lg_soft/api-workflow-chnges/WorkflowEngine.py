import os
import yaml
import boto3
import utils
import uuid
import flask
import pdb
import json
import constants
from airflow import dag
from flask import request
from InitScript import init
from sagemaker import Session
from StepFactory import StepFactory
from AirflowFactory import AirflowFactory
from stepfunctions.steps import Parallel, Pass, Catch
from stepfunctions.steps import Chain, FrozenGraph
from stepfunctions.workflow import Workflow, Execution
from DeployStackHelper import create_deployment_steps
from stepfunctions.steps import Choice, ChoiceRule, Pass
from urllib.request import urlopen
from AirflowApi.Taskstatus import Taskstatus


airflow_endpoint = constants.AIRFLOW_ENDPOINT
config_path = './config/'
BUCKET_NAME= constants.SC_S3_BUCKET
app = flask.Flask(__name__, static_url_path='')

"""
Given an id, fetches the config yaml from the DDB.
"""


@app.route('/static/require.js', methods=['GET'], endpoint="require_workflow")
def root():
    return app.send_static_file('require.js')


def fetch_config_from_db(config_id, type):
    return utils.fetch_config_from_db(config_id, type)
    # with open('./config-new/' + config_id + '.yaml') as file:
    #     return yaml.full_load(file)

def fetch_module_config_from_db(module_id, workflow_version_id):
    return utils.fetch_module_config_from_db(module_id, workflow_version_id)

def update_module_config(workflow_version_id,workflow_execution_id):
    return utils.update_module_config(workflow_version_id,workflow_execution_id)
# TODO: Fetch it from the DDB instead of hardcoding.

def fetch_deployment_config(workflow_version_id):
    return utils.fetch_deployment_config(workflow_version_id)
    # with open('./config-new/deployment.yaml') as file:
    #     return yaml.full_load(file)


def build_step(workflow_version_id, definition, context):
    module_id = definition.get("id")
    # config_loc =  definition.get("ConfigLocation")
    if definition.get("id") is not None:
        module_config = fetch_module_config_from_db(module_id, workflow_version_id)
        #config_loc = utils.generate_temp_config(platformModuleId, config_string)

        return StepFactory().getStepImplementation(context, module_config)

def airflow_step(workflow_version_id, definition, context, reuse_ecs_instance):

    module_id = definition.get("id")
    if definition.get("id") is not None:
        response = fetch_module_config_from_db(module_id, workflow_version_id)
        step_config = response.get('config')
        config_string = response.get('config_string')
        platformModuleId = response.get('platformModuleId')
        return AirflowFactory().getStepImplementation(context, platformModuleId, step_config, config_string, workflow_version_id,module_id, reuse_ecs_instance)

"""
This is the heart of the workflow engine. The pseudo code is as follows:

Step 1: For each Module in the Workflow Config:
    Fetch the Module Config yaml from DDB
    Create Deployment Artifacts required for the workflow.
    Build Step by using StepFactory class.

Step 2: Chain the Modules based on the Workflow Config "Step" Definition.
Step 3: Return the work flow definition.

"""


def build_workflow(workflow_version_id, workflow_execution_id, context, is_dry_run):

    # This is a request context object to store the context of the current workflow.
    # TODO: Fetch default  context details from the workflow config.
    # TODO: Fetch default  context details from the workflow config.
    context['pipeline_execution'] = {
            'BaseLocation': constants.S3_BASE_LOCATION,
            'sagemaker_session': Session(),
            'NotificationArn': constants.NOTIFICATION_TOPIC
        }

    context['step_execution'] = {}
    # Fetches the config yaml from DB
    config_yaml = fetch_config_from_db(workflow_version_id, constants.WORKFLOW_CONFIG_VERSION)
    deployment_steps = {}
    # List of Modules Referred in the Workflow Definition.
    modules = config_yaml.get('modules')
    steps_list = []

    #  If not Dry Run - Fetch the Deployment Config Add CFT Resource Creation Steps.
    if not is_dry_run:
        deployment_yaml = fetch_deployment_config(workflow_version_id) #Gets deployment details, name and type from module yaml for each module
        deployment_steps = create_deployment_steps(deployment_yaml, workflow_execution_id)
        # Create, Wait,  Get Status and Delete are Resource Creation Steps.
        # steps_list.append(deployment_steps['create'])
        # steps_list.append(deployment_steps['wait'])
        # steps_list.append(deployment_steps['get_status'])
        context['step_execution']['resource_names'] = deployment_steps['get_status']

    # For direct string testing, add a wait step for giving instance time to register itself to cluster
    if context['clusterName']:
        if utils.count_cluster_instances(context['clusterName']) == 0:
            steps_list.append(deployment_steps['wait'])

    # For Each module, build a step by using the definition and add it to the request context object.
    for step_name, step_definition in modules.items():
        if type(step_definition) is not str:
            step = build_step(workflow_version_id, step_definition, context)
            if type(step) is str:
                return step
            context['step_execution'][step_name] = step

    # Now that the modules are defined, iterate through the steps and assemble them.
    steps = utils.ordered_config(config_yaml.get('steps'))

    for step in steps:
        steps_list.append(context['step_execution'][step])

    # If not a Dry Run, then clean up the resources. In case of a dry run no resources are created anyways!
    # if not is_dry_run:
    #     steps_list.insert(len(steps_list) - 1, deployment_steps['delete'])

    # Wrapping all the states in one 'Parallel' branch so that finish step can run in all the cases.
    parallel_step = Parallel(state_id="Try")
    parallel_step.add_branch(Chain(steps_list))
    parallel_step.add_catch(Catch(error_equals=["States.ALL"],next_step=deployment_steps['finish']))

    # added finishing step to step Function
    # steps_list.append(deployment_steps['finish'])

    #removing None instances from steps_list
    # modified_steps_list = [i for i in steps_list if i]

    # adding finishing step to parallel step
    modified_steps_list = [parallel_step, deployment_steps['finish']]
    # Return a Data Science SDK Workflow object.
    workflow_name = config_yaml.get('metaData')['name'].replace(" ","")

    return Workflow(
        name=workflow_name + "-" + str(uuid.uuid1()),
        definition=Chain(modified_steps_list),
        role=constants.WORKFLOW_EXEC_ROLE
    )


def airflow_build_workflow(workflow_version_id, workflow_execution_id, context, is_dry_run):

    # This is a request context object to store the context of the current workflow.
    # TODO: Fetch default  context details from the workflow config.
    context['pipeline_execution'] = {
            'BaseLocation': constants.S3_BASE_LOCATION,
            'sagemaker_session': Session(),
            'NotificationArn': constants.NOTIFICATION_TOPIC
        }

    context['step_execution'] = {}
    # Fetches the config yaml from DB
    config_yaml = fetch_config_from_db(workflow_version_id, constants.WORKFLOW_CONFIG_VERSION)
    deployment_steps = {}
    # List of Modules Referred in the Workflow Definition.
    modules = config_yaml.get('modules')
    reuse_ecs_instance = config_yaml.get('general').get('reuse_ecs_instance')
    print('reuse ecs instance is',reuse_ecs_instance)
    steps_list = []
    
    #  If not Dry Run - Fetch the Deployment Config Add CFT Resource Creation Steps.
    if not is_dry_run:
        deployment_yaml = fetch_deployment_config(workflow_version_id)
        print("deployment_yaml",deployment_yaml)
        deployment_steps = create_deployment_steps(deployment_yaml, workflow_execution_id, context)
        # Create, Wait,  Get Status and Delete are Resource Creation Steps.
        steps_list.append(deployment_steps['create'])
        steps_list.append(deployment_steps['wait'])
        steps_list.append(deployment_steps['get_status'])
        context['step_execution']['resource_names'] = deployment_steps['get_status']
    # For Each module, build a step by using the definition and add it to the request context object.
    
    for step_name, step_definition in modules.items():

        if type(step_definition) is not str and constants.ORCHESTRATION_ENGINE == 'Airflow':
            if 'Choice' not in step_name:
                step = airflow_step(workflow_version_id, step_definition, context, reuse_ecs_instance)
                context['step_execution'][step_name] = step
        else:
            step = build_step(workflow_version_id, step_definition, context)
            context['step_execution'][step_name] = step

    # Now that the modules are defined, iterate through the steps and assemble them.
    steps = utils.airflow_ordered_config(config_yaml.get('steps'),context)
    
    steps_list = steps_list + steps

    # If not a Dry Run, then clean up the resources. In case of a dry run no resources are created anyways!
    if not is_dry_run:
        steps_list.append(deployment_steps['delete'])

    # # Return a Data Science SDK Workflow object.
        workflow_name = config_yaml.get('metaData')['name'].replace(" ","")

    modified_steps_list = [i for i in steps_list if i]
    return Workflow(
        name=workflow_name + "-" + str(uuid.uuid1()),
        definition=Chain(modified_steps_list),
        role=constants.WORKFLOW_EXEC_ROLE
    )


'''

This method is no-longer used in the UI.
The idea of this method is to see if the pipeline created in the UI has any syntax error or not.
This method - takes in the workflow config id and tries to build a pipeline by using step function api.
It will not execute / create a pipeline. It will only be used to build a pipeline.
If the above is successful, then method stores the definition and the flow chart diagram generated in the DDB.

'''


def dry_run_create_pipeline(workflow_version_id, context):
    pipeline = build_workflow(workflow_version_id, workflow_execution_id, context, False)

    if pipeline is None:
        return "Uh oh.. Looks like the Pipeline has some errors", 400

    pipeline.create()
    definition = pipeline.definition
    template = pipeline.get_cloudformation_template()

    html = '<script type="text/javascript" src="/static/require.js"></script>' + pipeline.render_graph().data

    utils.save_workflow_definition(workflow_version_id, definition, template, html)

    return template, 201


'''
This API is to visualize a Workflow Version in a SVG format. This is not used in the UI.
Suggestion is to use this API in the UI for debugging and visualization purposes.
'''


@app.route('/workflow/<workflow_version_id>/definition', endpoint="view_pipeline", methods=['GET'])
def view_pipeline(workflow_version_id):
    return utils.fetch_workflow_svg(workflow_version_id), 200


'''
Method takes in Workflow Config ID, builds step function definition and executes the same.
'''


def execute(workflow_version_id, context):

    
    workflow_execution_id = utils.check_and_update_execution_status(workflow_version_id, context)
    if not workflow_execution_id:
        return {"status":"error",
                "message":"Uh oh.. Looks like there is already a workflow under execution..Please try after sometime!",
                "execution_id":0}
    #final_output_loc = utils.update_last_module_loc(workflow_version_id,workflow_execution_id)
    #utils.update_output_loc_in_db(workflow_version_id, final_output_loc)
    context['tags']['executionId'] = str(workflow_execution_id) 
    utils.update_module_config_new_version(workflow_version_id,workflow_execution_id)
    final_output_loc = utils.get_project_output(workflow_version_id)
    utils.update_output_loc_in_db(workflow_version_id, final_output_loc,workflow_execution_id)


    if constants.ORCHESTRATION_ENGINE == 'Airflow':
        pipeline = airflow_build_workflow(workflow_version_id, workflow_execution_id, context, False)
    else:
        pipeline = build_workflow(workflow_version_id, workflow_execution_id, context,False)

    if type(pipeline) is str:
        return {"status":"error",
                "message":pipeline}

    # Data Science SDK that creates a pipeline and execute it.
    # Create will only create step function defintion.
    # Execution will result in actual state machine to be created.
    updated_definition = pipeline.definition.to_json().replace("TransformJobName","TransformJobName.$")
    print(updated_definition)

    if constants.ORCHESTRATION_ENGINE == 'Airflow':
        print('test')
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(BUCKET_NAME)
        path = 'public/airflow/definationyaml/'+str(workflow_execution_id)+'/definition.yaml'
        data = updated_definition
        bucket.put_object(ACL='public-read', ContentType='application/json', Key=path, Body=data,) 
        body = {"uploaded": "true", "bucket": BUCKET_NAME, "path": path,}
        airflow_dag = dag(workflow_execution_id, context)
        print("Exucution is completed")
        modified_pipeline = Workflow(
            name= pipeline.name,
            definition =FrozenGraph.from_json(updated_definition),
            role=constants.WORKFLOW_EXEC_ROLE
            )
        execution_dagid = "LG-dynamic-"+str(workflow_execution_id)
        template = modified_pipeline.get_cloudformation_template()
        html = "This is airflow no html page is required"
        utils.save_execution_results_in_db(workflow_execution_id, html, execution_dagid, template)
        #return 
        #{
        #"statusCode": 200,
        #"body": json.dumps(body)
        #}
    
    else:
        modified_pipeline = Workflow(
            name= pipeline.name,
            definition =FrozenGraph.from_json(updated_definition),
            role=constants.WORKFLOW_EXEC_ROLE,
            tags=[
                {
                    'key': 'user',
                    'value': context['tags']['user']
                },
                {
                    'key': 'team',
                    'value': context['tags']['team']
                },
                {
                    'key': 'department',
                    'value': context['tags']['department']
                },
                {
                    'key': 'executionId',
                    'value': context['tags']['executionId']
                }
            ]
            )
        modified_pipeline.create()
        modified_pipeline_flow = modified_pipeline.execute()
        template = modified_pipeline.get_cloudformation_template()
        html = '<script type="text/javascript" src="require.js"></script>' + modified_pipeline_flow.render_progress().data
        #Saving the Execution ARN, Resulting HTML Progress in the DDB
        utils.save_execution_results_in_db(workflow_execution_id, html, modified_pipeline_flow.execution_arn, template)

    return {"status":"accepted",
            "message": "Workflow Execution Created",
            "execution_id": int(workflow_execution_id)}


'''
This method is used for visualizing a workflow execution status.
It takes in Workflow Execution ID as input.
Queries the DDB to get the the details [execution arn and asl] of the execution.
Uses the Data Science SDK to render the execution details as a SVG.
Data Science SDK method - list_events() - will generate teh list of all the events that were executed as a HTML
Data Science SDK method - render_progress() - will generate a SVG of the state machine.
'''


@app.route('/workflow/<workflow_version_id>/execution/<workflow_execution_id>/status', endpoint="view_execution_status",
           methods=['GET'])
def view_execution_status(workflow_version_id, workflow_execution_id):
    client = boto3.client('stepfunctions')
    # Get Execution ARN from DDB, Return 404 if not found
    execution_arn = utils.get_execution_arn(workflow_version_id, workflow_execution_id)

    if execution_arn is None:
        return "Uh oh.. Looks like there is no execution trace..", 404

    # Get the details of the state machine from Boto3.
    state_machine_details = client.describe_execution(
        executionArn=execution_arn
    )

    workflow_execution_details = client.describe_state_machine(stateMachineArn=state_machine_details['stateMachineArn'])

    # Re-create a Workflow object and teh Execution object from the Step Function Execution details.
    workflow = Workflow(
        name=workflow_execution_details['name'],
        definition=FrozenGraph.from_json(workflow_execution_details['definition']),
        role=workflow_execution_details['roleArn'],
        state_machine_arn=workflow_execution_details['stateMachineArn'],
        client=client
    )

    execution = Execution(
        workflow=workflow,
        execution_arn=state_machine_details['executionArn'],
        start_date=state_machine_details['startDate'],
        status=state_machine_details['status'],
        client=client
    )

    # require.js is added to render the svg graphics. Its a workaround done to display the output of render_progress
    # in a browser.
    return '<script type="text/javascript" src="/static/require.js"></script>' + execution.list_events(html=True).data + execution.render_progress().data, 200


@app.route('/workflow/<workflow_version_id>/execution/<workflow_execution_id>/airflowstatus', endpoint="dag_execution_status",
           methods=['GET'])
def dag_execution_status(workflow_version_id, workflow_execution_id):
     
    final = {}
    dag_idd = "LG-dynamic-"+str(workflow_execution_id)
    url = 'http://{}:8080/api/experimental/dags/{}/dag_runs'.format(airflow_endpoint,dag_idd)
    dags_status = urlopen(url).read().decode("utf-8")
    #dags_status = os.popen('curl --silent http://{}:8080/api/experimental/dags/{}/dag_runs'.format(airflow_endpoint,dag_idd)).read()
    
    if "error" in dags_status:
        final['Dag_status'] = "initializing "
        final['Task_list'] = []
        obj = json.dumps(final, indent = 4)
        return obj
    else:
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


@app.route('/workflow/<workflow_version_id>/execution/<workflow_execution_id>/airflowtaskstatus', endpoint="dag_tasks_status",
           methods=['GET'])
def dag_tasks_status(workflow_version_id, workflow_execution_id):
    

    obj = Taskstatus(workflow_version_id, workflow_execution_id)
    return obj


@app.route('/workflow/<workflow_version_id>/execution/<workflow_execution_id>/airflowtasklogs', endpoint="dag_tasks_logs",
           methods=['GET'])
def dag_tasks_logs(workflow_version_id, workflow_execution_id):

    pass    
    

'''
This method is used to publish the CFT created to the Service Catalog API.
It takes in workflow config id as the input, builds workflow and then publishes the generated CFT to Service Catalog.
'''


def publish(workflow_version_id, context):
    pipeline = build_workflow(workflow_version_id, workflow_execution_id, context, False)
    template = pipeline.get_cloudformation_template()
    # template = template.replace("TransformJobName", "TransformJobName.$")
    # template = template.replace("S3Uri""", "S3Uri.$")
    response = utils.publish(template.replace("TransformJobName", "TransformJobName.$"), workflow_version_id)
    return response

'''
This method is responsible for handling module testing with direct string
'''
@app.route('/workflow/testmodule', endpoint="test_module",
           methods=['POST'])
def test_module():
    request_data = request.json
    action = request_data.get('action')
    # deployConfig = request_data.get('deployConfig')
    clusterName = request_data.get('clusterName')
    response = None
    context = {}
    try:
        if action == 'deploy':
            context['tags'] = {}
            context['tags']['user'] = request_data.get('user')
            context['tags']['team'] = request_data.get('team')
            context['tags']['department'] = request_data.get('department')
            response = utils.start_resource(context)
        elif action == 'terminate':
            response = utils.stop_resource(clusterName)
        else:
            response = {"status":"error", "message": "Please send a valid action"}
    except Exception as e:
        print("Error while processing", e)
        return json.dumps({"status":"error", "message": "Error in workflow engine"})

    return json.dumps(response)

'''
This is the entry point for workflow engine.
The input request looks like this:
    POST /workflow/1234567 HTTP/1.1
    Host: localhost:8080
    Content-Type: application/json

    {
    "action": "execute|dry-run|publish"
    }

    Based on the action - the appropriate internal methods will be called.
    It returns a success / throws error in case of failure.

'''


@app.route('/workflow/<workflow_version_id>', endpoint="process_request",
           methods=['POST'])
def process(workflow_version_id):
    request_data = request.json
    action = request_data.get('action')
    response = None
    context = {}
    try:
        if action == 'dry-run':
            response = dry_run_create_pipeline(workflow_version_id, context)
        elif action == 'publish':
            response = publish(workflow_version_id, context)
        elif action == 'execute':
            context['stringInputs'] = request_data.get('stringInputs')
            # context['dictionaryInputs'] = request_data.get('dictionaryInputs')
            context['clusterName'] = request_data.get('clusterName')
            context['executionDescription'] = request_data.get('executionDescription')
            context['scheduleId'] = request_data.get('scheduleId')
            context['tags'] = {}
            context['tags']['user'] = request_data.get('user')
            context['tags']['team'] = request_data.get('team')
            context['tags']['department'] = request_data.get('department')
            response = execute(workflow_version_id, context)
        else:
            response = {"status":"error", "message": "Please send a valid action"}
    except Exception as e:
        print("Error while processing", e.with_traceback())
        return json.dumps({"status":"error", "message": e.with_traceback()})

    return json.dumps(response)

if __name__ == "__main__":
    #init() #Commmented because not needed in actual deployment
    app.run(host="0.0.0.0", port=8080)
    # print(publish('1598592300'))
