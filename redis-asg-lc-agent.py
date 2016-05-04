#!/usr/bin/env python2.7

import boto3
import json
import os
import subprocess
import urllib2

queue_url           = os.environ['SQS_QUEUE_URL']
aws_region          = os.environ['AWS_REGION']
instid_ip_tablename = os.environ['INST_ID_TO_IP_TABLE']

REDIS_PORT          = 6379

metadata_inst_id_req = urllib2.Request('http://169.254.169.254/latest/meta-data/instance-id')
my_instance_id      = urllib2.urlopen( metadata_inst_id_req ).read()

boto3.setup_default_session(region_name=aws_region)

sqs   = boto3.client('sqs')
asg   = boto3.client('autoscaling')
ec2   = boto3.client('ec2')
ddb   = boto3.client('dynamodb')

def delete_message(message):
    message_id     = message['MessageId']
    receipt_handle = message['ReceiptHandle']
    print("Deleting message id=%s" % message_id)
    
    sqs.delete_message(
        QueueUrl = queue_url,
        ReceiptHandle = receipt_handle
    )

def update_inst_id_ip_table(instance_id, ip_address):
    
    ddb.put_item(
        TableName=instid_ip_tablename,
        Item={
            'instance_id': {
                'S' : instance_id,
            },
            'ip_address' : {
                'S' : ip_address
            }
        }
    )
    print("Updated DDB table %s = %s" % (instance_id, ip_address) )

def register_instance_ips(instance_ids):
    
    # Describe instances to find IP of each instance
    desc_inst_resp = ec2.describe_instances(
        InstanceIds=instance_ids
    )
    
    reservations = desc_inst_resp['Reservations']
    for reservation in reservations:
        for instance in reservation['Instances']:
            update_inst_id_ip_table(instance['InstanceId'], instance['PrivateIpAddress'])
    

def get_instance_ips(instance_ids):
    
    instance_ips = []
    
    # Describe instances to find IP of each instance
    desc_inst_resp = ec2.describe_instances(
        InstanceIds=instance_ids
    )
    
    reservations = desc_inst_resp['Reservations']
    for reservation in reservations:
        for instance in reservation['Instances']:
            instance_ips.append(instance['PrivateIpAddress'])
    
    return instance_ips
    

def create_cluster(asg_name, num_replicas, redis_port):
    print("Creating cluster")
    
    instance_ids = []
    
    # Describe ASG to get list of instance IDS
    desc_asg_resp = asg.describe_auto_scaling_groups(
        AutoScalingGroupNames=[
            asg_name
        ]
    )
    asg_group = desc_asg_resp['AutoScalingGroups'][0]
    instances = asg_group['Instances']
    print instances
    
    # Get instance ID of each instance
    for instance in instances:
        instance_ids.append(instance['InstanceId'])
    
    # Check if number of instances meets desired count
    desired_count = asg_group['DesiredCapacity']
    if len(instance_ids) != desired_count:
        raise Exception("Number of instances does not meet desired count")
    
    instance_ips = get_instance_ips(instance_ids)
    register_instance_ips(instance_ids)
    
    cmd = "/usr/local/bin/redis-trib.rb create --replicas " + str(num_replicas) + " "
    for instance_ip in instance_ips:
        cmd = cmd + instance_ip + ":" + str(redis_port) + " "
    cmd = cmd + " </tmp/yes"
    
    print("Command: " + cmd)
    
    # Create 'yes' file
    f = open('/tmp/yes', 'w+')
    f.write('yes')
    f.close()
    
    # Run the redis-trib.rb create command
    subprocess.check_call(cmd, shell=True)    
    
    #/usr/local/bin/redis-trib.rb create --replicas 0 172.31.6.212:6379 172.31.49.228:6379 172.31.19.183:6379 < yes

def handle_instance_launch(instance_id, lc_token):
    print "Handling new instance: %s" % instance_id
    if instance_id == my_instance_id:
        return
    
    instance_ids = []
    instance_ids.append(instance_id)
    
    register_instance_ips(instance_ids)
    instance_ips = get_instance_ips(instance_ids)
    
    instance_ip = instance_ips[0]
    
    cmd = "redis-cli CLUSTER MEET %s %s" % (instance_ip, REDIS_PORT)
    subprocess.check_call(cmd, shell=True)

def handle_message(message):
    body = json.loads(message['Body'].encode("ascii"))
        
    event         = body.get('Event')
    asg_name      = body.get('AutoScalingGroupName')
    instance_id   = body.get('EC2InstanceId')
    lc_token      = body.get('LifecycleActionToken')
    lc_hook       = body.get('LifecycleHookName')
    lc_transition = body.get('LifecycleTransition')
    
    num_replicas  = body.get('NumReplicas')
    redis_port    = body.get('RedisPort')
    
    if event == 'autoscaling:TEST_NOTIFICATION':
        print("Removing test notification")
        delete_message(message)
        return
    
    if event == 'CLUSTER_CREATE':
        print("Received CLUSTER_CREATE")
        create_cluster(asg_name, num_replicas, redis_port)
        delete_message(message)
    
    if lc_transition == 'autoscaling:EC2_INSTANCE_TERMINATING':
        print("terminated")
    
    if lc_transition == 'autoscaling:EC2_INSTANCE_LAUNCHING':
        print("launching")
        handle_instance_launch(instance_id, lc_token)
        delete_message(message)

def main():
    
    print("Polling for messages")
    messages = sqs.receive_message(
                                QueueUrl            = queue_url,
                                MaxNumberOfMessages = 10,
                                WaitTimeSeconds     = 20)

    if messages and messages.get('Messages'):
        try:
            for message in messages['Messages']:
                 handle_message(message)
        except Exception as e:
            print("Caught exception while processing message: ", e)

if __name__ == "__main__":
    main()
