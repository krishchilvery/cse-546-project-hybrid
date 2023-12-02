from collections import deque
from concurrent.futures import ProcessPoolExecutor
import httpx
import time
import boto3
import requests

ceph_access_key = 'ACCESS_KEY'
ceph_secret_key = 'SECRET_KEY'

input_bucket = "546-oneszeros-input"


class QueueManager:
    def __init__(self):
        self.queue = deque([])
        self.visited = set()
        self.retries = 3
        self.s3 = boto3.resource(
            's3',
            endpoint_url='http://10.0.2.15:8000',
            aws_access_key_id = ceph_access_key,
            aws_secret_access_key = ceph_secret_key
        )
        self.bucket = self.s3.Bucket(input_bucket)
        self.get_objects()
        print(self.queue)

    def get_objects(self):
        self.bucket.load()
        objects = self.bucket.objects.all()
        for o in objects:
            if o.key in self.queue or o.key in self.visited:
                continue
            self.queue.append(o.key)
    
    def pop(self, retry=3):
        if retry == 0:
            print("Either no more objects to S3. Checked for 3 retries")
            return None
        if len(self.queue) == 0:
                self.get_objects()
                if retry != 3:
                    time.sleep(1)
                self.pop(retry-1)
        item = self.queue.popleft()
        self.visited.add(item)
        return item

    @property
    def len(self):
        return len(self.queue)

faas_url = "http://192.168.49.2:31112/function/handler"
username = "admin"
password = "xxA1F6p0HpDq"

executor = ProcessPoolExecutor(max_workers=4)

def invoke_function(key):
    print(f"Invoking for key: {key}")
    resp = requests.post(faas_url, data=key, headers={'Content-Type': 'text/plain'})

q = QueueManager()

while True:
    try:
        key = q.pop()
    except IndexError:
        print("Waiting for 2 seconds")
        time.sleep(2)
        continue

    executor.submit(invoke_function, key)


