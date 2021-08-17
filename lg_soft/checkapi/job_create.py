import requests
import json
url = 'http://ec2-workflow-engine-service-lb-22f51df475943c2e.elb.ap-southeast-1.amazonaws.com/workflow/execution/creategluejob'
myobj = {
        "job_name" : "searcetestmultitable",
        "job_schedule" : "None",
        "immediate_start" : "true",
        "tables": [{"catalog_database" : "searce",
                "catalog_table" : "source_test",
                "BookmarkKey" : "PersonID",
                "columns" : ["PersonID","LastName","FirstName","Address"],
                "destination_connection" : "destination-connection",
                "source_connection": "source-connection",
                "destination_database" : "dest",
                "destination_table" : "test"
                },
                {
                "catalog_database" : "searce",
                "catalog_table" : "source_employee",
                "BookmarkKey" : "EmpID",
                "columns" : ["EmpID","LastName","FirstName"],
                "destination_connection" : "destination-connection",
                "source_connection": "source-connection",
                "destination_database" : "dest",
                "destination_table" : "employee"
                },
                {
                "catalog_database" : "searce",
                "catalog_table" : "source_student",
                "BookmarkKey" : "StudentID",
                "columns" : ["PersonID","LastName"],
                "destination_connection" : "destination-connection",
                "source_connection": "source-connection",
                "destination_database" : "dest",
                "destination_table" : "student"
                }]
        }

headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

x = requests.post(url, data = json.dumps(myobj), headers=headers)

print(x.text)
