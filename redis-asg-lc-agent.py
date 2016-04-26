#!/usr/bin/env python2.7

import boto3
import json
import os

queue_url  = os.environ['SQS_QUEUE_URL']
aws_region = os.environ['AWS_REGION']

sqs   = boto3.client('sqs')

def delete_message(message):
    message_id     = message['MessageId']
    receipt_handle = message['ReceiptHandle']
    print "Deleting message id=%s" % message_id
    
    sqs.delete_message(
        QueueUrl = queue_url,
        ReceiptHandle = receipt_handle
    )

def handle_message(message):
    body = json.loads(message['Body'].encode("ascii"))
    
    print body
    
    event  = body.get('Event')
    
    if event == 'autoscaling:TEST_NOTIFICATION':
        print "Removing test notification"
        delete_message(message)
        return
    
    if event == 'CLUSTER_CREATE':
        print "Received CLUSTER_CREATE"
        return
    
    instance_id   = body.get('EC2InstanceId')
    asg_name      = body.get('AutoScalingGroupName')
    lc_token      = body.get('LifecycleActionToken')
    lc_hook       = body.get('LifecycleHookName')
    lc_transition = body.get('LifecycleTransition')
    
    if lc_transition == 'autoscaling:EC2_INSTANCE_TERMINATING':
        print "terminated"
    
    if lc_transition == 'autoscaling:EC2_INSTANCE_LAUNCHING':
        print "launching"
    
    

def main():
    
    while(True):
        print "--- Polling for messages ---"
        messages = sqs.receive_message(
                                    QueueUrl            = queue_url,
                                    MaxNumberOfMessages = 1,
                                    WaitTimeSeconds     = 20)
    
        if messages and messages.get('Messages'):
            try:
                handle_message(messages['Messages'][0])
            except:
                print "Unable to handle message"

if __name__ == "__main__":
    main()
