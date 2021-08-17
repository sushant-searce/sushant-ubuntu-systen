import requests

url = 'http://ec2-workflow-engine-service-lb-22f51df475943c2e.elb.ap-southeast-1.amazonaws.com/workflow/execution/listgluejob'
myobj = {'database_name': 'searcev2'}

x = requests.post(url, data = myobj)

print(x.text)
