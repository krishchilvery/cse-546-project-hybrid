import boto3
from boto3 import client as boto3_client
import os
import re
import click

input_bucket = "546-oneszeros-input"
output_bucket = "546-oneszeros-output"
test_cases = "test_cases/"


s3 = boto3.resource(
        's3',
        endpoint_url='http://10.0.2.15:8000',
        aws_access_key_id = "ACCESS_KEY",
        aws_secret_access_key = "SECRET_KEY"
)

@click.group()
def cli():
	pass

def clear_input_bucket():
	global input_bucket
	bucket = s3.Bucket(input_bucket)
	objects = bucket.objects.all()
	objects = [{"Key": o.key} for o in objects]
	try:
		bucket.delete_objects(Delete={'Objects':objects})
	except Exception as e:
		print(e)
		print("Nothing to clear in input bucket")
	
def clear_output_bucket():
	global output_bucket
	bucket = s3.Bucket(output_bucket)
	objects = bucket.objects.all()
	objects = [{"Key": o.key} for o in objects]
	try:
		bucket.delete_objects(Delete={'Objects':objects})
	except Exception as e:
		print(e)
		print("Nothing to clear in output bucket")

def upload_to_input_bucket_s3(path, name):
	global input_bucket
	bucket = s3.Bucket(input_bucket)
	bucket.upload_file(path + name, name)
	
	
def upload_files(test_case):	
	global input_bucket
	global output_bucket
	global test_cases
	
	
	# Directory of test case
	test_dir = test_cases + test_case + "/"
	
	# Iterate over each video
	# Upload to S3 input bucket
	for filename in os.listdir(test_dir):
		if filename.endswith(".mp4") or filename.endswith(".MP4"):
			print("Uploading to input bucket..  name: " + str(filename)) 
			upload_to_input_bucket_s3(test_dir, filename)


@cli.command()
def clear_buckets():
	clear_input_bucket()
	clear_output_bucket()


def read_mapping():
	results = []
	with open("mapping", "r") as f:
		for line in f.readlines():
			line = line.strip()
			filename, major, year = re.split(":|,", line)
			key = filename.split(".")[0]	
			if line:
				results.append((key, major, year))
	return results

@cli.command()
def verify_outputs():
	global output_bucket
	expected_results = read_mapping()
	bucket = s3.Bucket(output_bucket)
	total = len(expected_results)
	count = 0
	for key, major, year in expected_results:
		obj = s3.get_object(Bucket=output_bucket, Key=key)
		result = obj["Body"].read().decode("utf-8")
		result = result.strip()
		name, predicted_major, predicted_year = result.split(",")
		if predicted_major != major or predicted_year != year:
			print("Error in output for " + key)
			print("Expected: " + major + ", " + year)
			print("Got: " + predicted_major + ", " + predicted_year)
		else:
			print("Verified output for " + key)
			count += 1
	print("Total: " + str(total))
	print("Verified: " + str(count))
	print("Accuracy: " + str(count/total))

@cli.command()
def list_buckets():
	global input_bucket, output_bucket
	bucket = s3.Bucket(input_bucket)
	print("INPUT BUCKET")
	objects = bucket.objects.all()
	for o in objects:
		print(o.key)

	print()
	print("OUTPUT_BUCKET")
	bucket = s3.Bucket(output_bucket)
	objects = bucket.objects.all()
	for o in objects:
		print(o.key)

@cli.command()
@click.option('--key', prompt =  "Enter the Key you want to download: ")
def download_output(key):
	bucket = s3.Bucket(output_bucket)
	bucket.download_file(key, key.split('.')[0] + '.csv')


@cli.command()
def workload_generator():

	# print("Running Test Case 0")
	# upload_files("test_case_0")
	
	print("Running Test Case 1")
	upload_files("test_case_1")

	print("Running Test Case 2")
	upload_files("test_case_2")

	

# First Run the workload generator
# Then run the verify outputs
if __name__ == "__main__":
	cli()
	# clear_input_bucket()
	# clear_output_bucket()	
	# workload_generator()	
	# verify_outputs()
	

