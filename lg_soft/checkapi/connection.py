import requests
import json

url = 'http://ec2-workflow-engine-service-lb-22f51df475943c2e.elb.ap-southeast-1.amazonaws.com/workflow/execution/createconnection'

myobj = {'Name':'destination-connection',
        'mysql_connection_endpoint': 'glue-dms-database.c9zobpcmtbei.ap-southeast-1.rds.amazonaws.com',
        'mysql_port': '3306',
        'mysql_db': 'dest',
        'mysql_username': 'admin',
        'mysql_password': 'password',
        'connection_type': 'destination',
        'SubnetId': 'subnet-015d723fbc480ee95',
        'SecurityGroupIdList': 'sg-09f57bab9841079de',
        'AvailabilityZone': 'ap-southeast-1a'}
#data_json = json.dumps(obj)

x = requests.post(url, data = myobj)

print(x.text)
