import requests

url = 'http://ec2-workflow-engine-service-lb-22f51df475943c2e.elb.ap-southeast-1.amazonaws.com/workflow/execution/schedulegluejob'
myobj = {
        'glue_job_name': 'jobv2',
        'schedule_cron' : 'cron(15 12 * * ? *)',
        'schedule_name' : 'testing_ctrigg'
        }

x = requests.post(url, data = myobj)

print(x.text)
