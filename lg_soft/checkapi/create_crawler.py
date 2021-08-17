import requests

url = 'http://ec2-workflow-engine-service-lb-22f51df475943c2e.elb.ap-southeast-1.amazonaws.com/workflow/execution/createcrawler'
myobj = {
        'crawler_name': 'testingconnectioncraw',
        'catalog_db_name' : 'searce',
        'connection_name' : 'source-connection',
        'path' : 'source'}

x = requests.post(url, data = myobj)

print(x.text)
