## Data Loader Lambda

This lambda does the following:
1. Loads a CSV file from S3
2. Validates if the CSV is as per schema, drops the rows if anything not matching. 
3. Does any data transformations. 
4. Writes the cleansed output into a S3 location. 

This lambda uses the following: 

1. [AWS Data Wrangler library](https://github.com/awslabs/aws-data-wrangler) to read, write into S3.

2. It uses awesome [pandas schema](https://tmiguelt.github.io/PandasSchema/) to do the validation. 

3. Uses [Serverless](https://www.serverless.com/) framework for lambda deployments.  

### Installation

Step 1: Create a S3 Bucket which will be used for this project. 

```

aws s3api create-bucket --bucket sagemaker-mlops-demo-lg-jul-2020 --region us-east-1

```

Step 2: Install AWS Data Wrangler Lambda Layer.

```bash
wget https://github.com/awslabs/aws-data-wrangler/releases/download/1.6.3/awswrangler-layer-1.6.3-py3.6.zip
aws s3 cp awswrangler-layer-1.6.3-py3.6.zip s3://sagemaker-mlops-demo-lg-jul-2020/awswrangler-layer-1.6.3-py3.6.zip

aws lambda publish-layer-version \
    --layer-name aws-data-wrangler \
    --description "Panda, numpy, aws data wrangler suite" \
    --license-info "MIT" \
    --content S3Bucket=sagemaker-mlops-demo-lg-jul-2020,S3Key=awswrangler-layer-1.6.3-py3.6.zip \
    --compatible-runtimes python3.6

```

Note down the LayerVersionArn from the above command and change in the serverless yaml. 

Step  3: Deploy Lambda: 

```
serverless deploy
```

Step4: Test: 

```

serverless invoke --function DataValidator --path mock_input.json

```