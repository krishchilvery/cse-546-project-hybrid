import boto3
import json
import os
import time

access_key = "AKIAZTTHAE5PD7ISLVNT"
secret_key = "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"

region = "us-east-1"
os.environ["AWS_DEFAULT_REGION"] = region

dynamodb = boto3.resource("dynamodb", aws_access_key_id=access_key, aws_secret_access_key=secret_key)

student_data_path = "student_data.json"
table_name = "546StudentData"

input_bucket = "546proj2-oneszeros"
output_bucket = "546proj2output-oneszeros"

lambda_image_name = "cse546projecthybrid:latest"

def create_dynamodb():
    try:
        table = dynamodb.Table(table_name)
        print("Table already exists")
    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
        table = dynamodb.create_table(
            TableName = table_name,
            KeySchema = [
                {
                    "AttributeName": "id",
                    "KeyType": "HASH"
                }
            ],
            AttributeDefinitions = [
                {
                    "AttributeName": "id",
                    "AttributeType": "N"
                }
            ],
            ProvisionedThroughput = {
                "ReadCapacityUnits": 5,
                "WriteCapacityUnits": 5
            }
        )
        table.meta.client.get_waiter("table_exists").wait(TableName = table_name)
        print("Successfully created table")

    with open(student_data_path) as f:
        student_data = json.load(f)
        for student in student_data:
            table.put_item(Item = student)


if __name__ == "__main__":
    create_dynamodb()
