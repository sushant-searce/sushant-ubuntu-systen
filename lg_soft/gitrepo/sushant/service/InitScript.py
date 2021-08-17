import json
import boto3

import constants

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(constants.DYNMODB_TABLE)

'''

This file  seeds data  into the Dynamodb. 
It creates the following:
1. A sample workflow config. 
2. Sample Platform Modules. 
3. Sample Module configs. 

'''
def init():
    print("Running Init Scripts::: Seeding Sample Data:::", constants.DYNMODB_TABLE)
    if table.item_count >= 0:
        with open("./config-new - ddb/seed_database.json") as input_data:
            workflow_example_items = json.load(input_data)['workflow_examples']
            for item in workflow_example_items:
                moduleConfig = item['moduleConfig'].replace("~~S3_BASE_BUCKET_NAME~~",
                                                            constants.S3_BASE_BUCKET_NAME)\
                                                            .replace("~~LAYERS_NAME~~",constants.LAYERS_NAME).replace("~~MODEL_NAME~~",constants.MODEL_NAME)

                if(item['type']== "workflowVersion"):
                    table.put_item(Item={
                        'id': item['id'],
                        'type': item['type'],
                        'workflowVersionConfig': moduleConfig,
                        'parent': item['parent'],
                        'updatedAt': item['updatedAt'],
                        'userName': item['userName'],
                        'userRole': item['userRole'],
                        'workflowVersionDescription': item['workflowVersionDescription'],
                        'workflowVersionName': item['workflowVersionName'],
                        'createdAt': '2020-07-14T05:47:09.302Z'
                    })
                elif item['type']== "moduleConfigVersion" :
                    table.put_item(Item={
                        'id': item['id'],
                        'type': item['type'],
                        'moduleConfig': moduleConfig
                    })
                elif item['type'] == 'workflow' :
                    table.put_item(Item={
                        'id': item['id'],
                        'type': item['type'],
                        'moduleConfig': moduleConfig,
                        'parent': item['parent'],
                        'updatedAt': item['updatedAt'],
                        'userName': item['userName'],
                        'userRole': item['userRole'],
                        'createdAt': '2020-07-14T05:47:09.302Z'
                    })
                else:
                    table.put_item(Item={
                        'id': item['id'],
                        'type': item['type'],
                        'moduleConfig': moduleConfig,
                        'moduleDescription': item['moduleDescription'],
                        'moduleName': item['moduleName'],
                        'moduleRelease': item['moduleRelease'],
                        'parent': item['parent'],
                        'updatedAt': item['updatedAt'],
                        'userName': item['userName'],
                        'userRole': item['userRole'],
                        'createdAt': '2020-07-14T05:47:09.302Z'
                    })