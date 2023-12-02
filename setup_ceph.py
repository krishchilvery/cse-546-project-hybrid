import boto
import boto.s3.connection
access_key = 'OG5CJ3AJ7SB12MB6WRKY'
secret_key = 'nFIuCfZpWnJCavHhDniYn4t3fFbhkEQr3WD7h25q'

conn = boto.connect_s3(
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
        host = '10.0.2.15',
	port = 8000,
        is_secure=False,               # uncomment if you are not using ssl
        calling_format = boto.s3.connection.OrdinaryCallingFormat()
        )

input_bucket_name = "546-oneszeros-input"
output_bucket_name = "546-oneszeros-output"

input_bucket = conn.create_bucket(input_bucket_name)
output_bucket = conn.create_bucket(output_bucket_name)
