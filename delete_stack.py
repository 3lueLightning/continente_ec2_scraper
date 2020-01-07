#this script is run at the vary end of the code to delete the current cloudformation stack
import boto3, requests

response = requests.get('http://169.254.169.254/latest/meta-data/instance-id')
instance_id = response.text

ec2 = boto3.client('ec2')
instance_attributes = ec2.describe_instances(InstanceIds=[instance_id])
for item_0 in instance_attributes['Reservations']:
	if 'Instances' in item_0.keys():
		for item_1 in item_0['Instances']:
			if 'Tags' in item_1.keys(): 
				stack_name = [tag['Value'] for tag in item_1['Tags'] if tag['Key'] == 'aws:cloudformation:stack-name'][0]
				break

cf = boto3.client('cloudformation')
cf.delete_stack(StackName=stack_name)
