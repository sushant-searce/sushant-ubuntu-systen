account = "797237262327"
region = "ap-southeast-1"
repo_name = "sagemaker_pytorch"
output_path = 's3://{}/output'.format("surya-test-bucket-2")
image_name  = '{}.dkr.ecr.{}.amazonaws.com/{}:latest'.format(account, region, repo_name)
role = "arn:aws:iam::797237262327:role/sagemaker-modules-sagemakerexecutionrole0D136CE5-120GAFR3HTKCW"
train_instance_type = "p3.2xlarge"
train_input_path = "s3://surya-test-bucket-2/input/train/"
validation_input_path = "s3://surya-test-bucket-2/input/validation/"


import sagemaker

sagemaker_session = sagemaker.Session()
sess = sagemaker.Session(


print(output_path)
print(image_name)

estimator = sagemaker.estimator.Estimator(
                       image_name=image_name,
                       base_job_name=base_job_name,
                       role=role,
                       train_instance_count=1,
                       train_instance_type=train_instance_type,
                       output_path=output_path,
                       sagemaker_session=sess)

estimator.set_hyperparameters(lr=0.01, epochs=10, batch_size=batch_size)

estimator.fit({'training': train_input_path, 'validation': validation_input_path})