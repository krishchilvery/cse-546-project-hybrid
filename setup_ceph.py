import boto3

access_key = 'ACCESS_KEY'
secret_key = 'SECRET_KEY'

conn = boto3.resource(
        's3',
        endpoint_url='http://10.0.2.15:8000',
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key
        )

input_bucket_name = "546-oneszeros-input"
output_bucket_name = "546-oneszeros-output"

input_bucket = conn.create_bucket(Bucket = input_bucket_name)
output_bucket = conn.create_bucket(Bucket = output_bucket_name)
