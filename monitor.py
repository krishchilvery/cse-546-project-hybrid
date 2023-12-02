ceph_access_key = 'ACCESS_KEY'
ceph_secret_key = 'SECRET_KEY'



class QueueManager:
    def __init__(self):
        self.queue = []
        self.s3 = boto3.resource(
            's3',
            endpoint_url='http://10.0.2.15:8000',
            aws_access_key_id = access_key,
            aws_secret_access_key = secret_key
        )