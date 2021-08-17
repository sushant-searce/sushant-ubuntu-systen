import boto3
s3client = boto3.client('s3')
region = s3client.meta.region_name

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table("searce-testing-table")
cft_name="cft-1111"

try:
    response = table.update_item(
            Key={"workflow_execution_id": 9876},
            UpdateExpression="SET cft_template = list_append(cft_template, :list)",            
            ExpressionAttributeValues={':list': [cft_name]},
            ReturnValues="UPDATED_NEW"
        )
    print(response)

except:
    response = table.update_item(
                Key={"workflow_execution_id": 9876},
                UpdateExpression='SET cft_template = :list',
                ExpressionAttributeValues={':list': [cft_name]},
                ReturnValues="UPDATED_NEW"
            )
    print(response)

########################################################################################

import boto3
s3client = boto3.client('s3')
region = s3client.meta.region_name

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table("searce-testing-table")

response = table.get_item(
            Key={
                "workflow_execution_id": 3796
                },)

print(response['Item']['cft_template'])