import yaml
import json
import boto3
import uuid
import base64
import time
import os
import zipfile
import subprocess
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook

def Convert(a):
    it = iter(a)
    res_dct = dict(zip(it, it))
    return res_dct

def checkKey(dict, key):

    if key in dict:
        pass
    else:
        raise ValueError('keyError: {0}, parameters are not set properly to run the job. Please add parameter- {0}'.format(key))

def create_glue_job(job_step, Script, job_parameters, context, jobMaxCapacity, jobTimeout, jobExecuteType):
    s3client = boto3.client('s3')
    region = s3client.meta.region_name
    client = boto3.client('glue', region_name=region)
    response = client.create_job(
                Name=job_step,
                Description='testing job for searce team',
                Role='arn:aws:iam::797237262327:role/lg-dev-glueServiceRole',
                GlueVersion='2.0',
                ExecutionProperty={'MaxConcurrentRuns': 5},
                Command={'Name': jobExecuteType, 'ScriptLocation': Script ,'PythonVersion': '3'},
                DefaultArguments=job_parameters,
                Tags=context,
                MaxRetries=1,
                Timeout=jobTimeout,
                NumberOfWorkers=jobMaxCapacity,
                WorkerType='Standard'
            )
    time.sleep(2)
    print('Glue job creates-: {}'.format(response)) 

class GlueOperatorBuilder:

    def callableFunction(self,taskid,states,context,DYNMODB_TABLE_NAME,workflow_execution_id,**kwargs):

        s3client = boto3.client('s3')
        region = s3client.meta.region_name
        print('region is',region)
        dynamodb = boto3.resource('dynamodb',region)
        ssmclient = boto3.client('ssm',region)
        dynamodbtable = ssmclient.get_parameter(
            Name='DYNMODB_TABLE'
        )['Parameter']['Value']
        table = dynamodb.Table(dynamodbtable)
        key = {
            "id": int(workflow_execution_id),
            "type": "workflowExecution"
        }
        tasksarns = []
        responsedb = table.get_item(Key=key)['Item']
        if(responsedb.get('tags')):
            for items in responsedb.get('tags'):
                tasksarns.append(items)
        print(tasksarns)
        taskname = taskid.replace('_',' ')
        print(states[taskname]['Parameters'])
        #job_st = states[taskname]['Parameters']['jobName']
        #job_step = job_st+str(uuid.uuid4().hex[:4])
        job_step = "searcetest"+str(uuid.uuid4().hex[:4])
        jobMaxCapacity = 10 or states[taskname]['Parameters']['Payload']['jobMaxCapacity']
        jobTimeout = 60 or states[taskname]['Parameters']['Payload']['jobTimeout']
        jobExecuteType = 'glueetl'or states[taskname]['Parameters']['Payload']['jobExecuteType']
        Script = states[taskname]['Parameters']['Payload']['jobScriptLocation']
        argument = states[taskname]['Parameters']['Payload']['arguments'][0]   
        jobType= states[taskname]['Parameters']['Payload']['jobType']     
        args = []
        for i in argument:
            a = i.replace('--', '')
            args.append(a)
        
        create_job_dict = Convert(args)
        run_job_dict =  Convert(argument)
        
        client = boto3.client('glue', region_name=region)
        
        if jobType=='mysql_to_mysql':
            mysql_to_mysql={}
            parameters = ['--mysql_source_endpoint', '--mysql_source_port', '--mysql_source_host_username', '--mysql_source_host_password', '--source_database', '--source_table', '--mysql_destination_endpoint', '--mysql_destination_port', '--mysql_destination_host_username', '--mysql_destination_host_password', '--destination_database', '--destination_table', '--columns']
            for key in parameters:
                checkKey(run_job_dict, key)
            for (key, value) in run_job_dict.items():
                if key in parameters:
                    mysql_to_mysql[key] = value
            
            mysql_to_mysql['--jobType'] = jobType

            create_glue_job(job_step, Script, mysql_to_mysql, context, jobMaxCapacity, jobTimeout, jobExecuteType)

            run_job_client = boto3.client('glue', region)
            response_v1 = run_job_client.start_job_run(
                 JobName=job_step,
                 Timeout=jobTimeout,
                 Arguments=run_job_dict,
                 WorkerType='Standard',
                 NumberOfWorkers=jobMaxCapacity
                 )

            print(response_v1)
            print("Glue job has been submitted!!")
            time.sleep(5)
            while True:
                job_status = run_job_client.get_job_run(
                         JobName=job_step,
                         RunId=response_v1.get('JobRunId'),
                         PredecessorsIncluded=False
                         )
                print(job_status)
                status = job_status.get('JobRun').get('JobRunState')
                time.sleep(2)
                if status =='SUCCEEDED': # or status =='RUNNING':

                    delete_response = run_job_client.delete_job(
                              JobName=job_step,
                                )
                    break
                elif status == 'FAILED':
                    del_response = run_job_client.delete_job(
                               JobName=job_step,
                               )
                    raise ValueError('Job run fail on Glue')
            print("Glue execution is comeplete")

        elif jobType=='mysql_to_s3':
            mysql_to_s3={}
            parameters = ['--mysql_source_endpoint', '--mysql_source_port', '--mysql_source_host_username', '--mysql_source_host_password', '--source_database', '--source_table', '--columns', '--s3_path_to_store_data', '--format']
        
            run_job_dict["--s3_path_to_store_data"] = run_job_dict["--outputfile"] 
            del run_job_dict["--outputfile"]
            print(run_job_dict)
            for key in parameters:
                checkKey(run_job_dict, key)
            for (key, value) in run_job_dict.items():
                if key in parameters:
                    mysql_to_s3[key] = value
            
            mysql_to_s3['--jobType'] = jobType
            create_glue_job(job_step, Script, mysql_to_s3, context, jobMaxCapacity, jobTimeout, jobExecuteType)
            
            run_s3_job_client = boto3.client('glue', region)
            response_v1 = run_s3_job_client.start_job_run(
                 JobName=job_step,
                 Timeout=jobTimeout,
                 Arguments=run_job_dict,
                 WorkerType='Standard',
                 NumberOfWorkers=jobMaxCapacity
                 )

            print(response_v1)
            while True:
                job_status = run_s3_job_client.get_job_run(
                        JobName=job_step,
                        RunId=response_v1.get('JobRunId'),
                        PredecessorsIncluded=False
                        )
                print(job_status)
                status = job_status.get('JobRun').get('JobRunState')
                time.sleep(2)
                if status =='SUCCEEDED': # or status =='RUNNING':
                    s3_location = run_job_dict["--s3_path_to_store_data"]
                    name_bucket = s3_location.split('/')[2]
                    name_key = '/'.join(s3_location.split('/')[3:])
                    s3_client = boto3.client('s3')
                    s3 = boto3.resource('s3')
                    my_bucket = s3.Bucket(name_bucket)
                    s3_key = '/'.join(name_key)

                    for object_summary in my_bucket.objects.filter(Prefix=name_key):
                        a = object_summary.key
                        result = a.startswith(name_key+"/run")#(name_key+'/part')
                        if result==True:
                            file_name = a
                            print(file_name)


                    copy_source = {
                        'Bucket': name_bucket,
                        'Key': file_name
                    }

                    s3.meta.client.copy(copy_source, name_bucket , name_key)

                    delete_response = run_s3_job_client.delete_job(
                                JobName=job_step,
                                )
                    break
                elif status == 'FAILED':
                    del_response = run_s3_job_client.delete_job(
                               JobName=job_step,
                               )
                    raise ValueError('Job run fail on Glue')
            print("Glue execution is comeplete")

        elif jobType=="gluejob" or "glueJob":
            try:
                script_location = os.path.split(Script)
                s3_location = script_location[0]
                name_bucket = s3_location.split('/')[2]
                name_key = '/'.join(s3_location.split('/')[3:])
                path = "/root/airflow/glue/execution-"+context['executionId']
                cmd="mkdir -p {}".format(path)
                out=subprocess.check_output(cmd,shell=True)
                s3_client = boto3.resource('s3')
                bucket = s3_client.Bucket(name_bucket)
                folder=name_key.split('/')[-1]
                for obj in bucket.objects.filter(Prefix = name_key):
                    new_path=path
                    full_path=obj.key.split('/')
                    for i in range(len(full_path)):
                        if full_path[i] == folder:
                            k=i+1
                            break
                    for i in range(k,len(full_path)-1):
                        new_path = new_path+"/"+full_path[i]
                    print("new_path",new_path)
                    cmd="mkdir -p "+new_path
                    os.system(cmd)
                    name=obj.key.split('/')[-1]
                    new_path=new_path+"/"+name
                    bucket.download_file(obj.key, new_path)

                primary_zip = "support-"+context['executionId']+".zip"
                cmd="cd "+path+"&& "+"zip -r "+primary_zip+" *"
                out=subprocess.check_output(cmd,shell=True)
                new_path=path+"/python_dependencies"
                cmd="mkdir -p " + new_path
                out=subprocess.check_output(cmd,shell=True)
                try:
                    cmd="cd /root/.pyenv/shims && ./python3.7 -m pip install -r " +path+"/requirements.txt -t "+new_path
                    out=subprocess.check_output(cmd,shell=True)
                except:
                    cmd="cd /root/.pyenv/shims && ./python3.7 -m pip install -r " +path+"/"+"requirement.txt -t "+new_path
                    print(cmd)
                    out=subprocess.check_output(cmd,shell=True)
                #cmd="cd "+path+" && "+"pip3 install -r requirement.txt -t "+new_path
                secondary_zip = "depend-"+context['executionId']+".zip"
                cmd="touch "+new_path+"/__init__.py"
                out=subprocess.check_output(cmd,shell=True)
                cmd="cd "+new_path+"&& "+"zip -r "+secondary_zip+" *"
                out=subprocess.check_output(cmd,shell=True)
                zip_location=path+"/"+primary_zip
                zip_location2=new_path+"/"+secondary_zip
                s3__client = boto3.client('s3')
                support_object_name ='public/Workflow/ExecutionFiles/{0}/{1}'.format(context['executionId'],'support.zip')
                response = s3__client.upload_file(zip_location, name_bucket, support_object_name)
                package_object_name ='public/Workflow/ExecutionFiles/{0}/{1}'.format(context['executionId'],'package.zip')
                response = s3__client.upload_file(zip_location2, name_bucket, package_object_name)
                run_job_dict["--extra-py-files"] = "s3://{0}/{1},s3://{0}/{2}".format(name_bucket, support_object_name, package_object_name)
                create_glue_job(job_step, Script, run_job_dict, context, jobMaxCapacity, jobTimeout, jobExecuteType)
                cmd="rm -rf "+path
                out=subprocess.check_output(cmd,shell=True)            
            except:
                create_glue_job(job_step, Script, run_job_dict, context, jobMaxCapacity, jobTimeout, jobExecuteType)
            
            run_job_client = boto3.client('glue', region)
            response_v1 = run_job_client.start_job_run(
                 JobName=job_step,
                 Timeout=jobTimeout,
                 Arguments=run_job_dict,
                 WorkerType='Standard',
                 NumberOfWorkers=jobMaxCapacity
                 )

            print(response_v1)
            print("Glue job has been submitted!!")
            tasksarns.append(response_v1['JobRunId']+"_DeploymentGLUE")
            print(tasksarns)
            responsedb['tags']=tasksarns
            print(responsedb)
            table.put_item(Item=responsedb)
            time.sleep(5)
            while True:
                job_status = run_job_client.get_job_run(
                         JobName=job_step,
                         RunId=response_v1.get('JobRunId'),
                         PredecessorsIncluded=False
                         )
                print(job_status)
                status = job_status.get('JobRun').get('JobRunState')
                time.sleep(2)
                if status =='SUCCEEDED': # or status =='RUNNING':
                   
                    delete_response = run_job_client.delete_job(
                              JobName=job_step,
                                )
                    break
                elif status == 'FAILED':
                    del_response = run_job_client.delete_job(
                               JobName=job_step,
                               )
                    raise ValueError('Job run fail on Glue')
            print("Glue execution is comeplete")

    def getOperator(self, dag, task_id, states, context, DYNMODB_TABLE_NAME, workflow_execution_id):
        operator = PythonOperator(
            task_id = task_id,
            provide_context=True,
            python_callable = GlueOperatorBuilder().callableFunction,
            op_kwargs={'taskid': task_id, 'states': states, 'context': context, 'DYNMODB_TABLE_NAME': DYNMODB_TABLE_NAME, 'workflow_execution_id':workflow_execution_id},
            dag = dag,
        )
        return operator
