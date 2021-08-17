import requests

url = 'http://ec2-workflow-engine-service-lb-22f51df475943c2e.elb.ap-southeast-1.amazonaws.com/workflow/execution/deletegluejob'
myobj = {'glue_job_name': 'searcetestmultitable'}

x = requests.post(url, data = myobj)

print(x.text)
