#!/usr/bin/env python2.7

import boto3
import json
import os
import subprocess
import sys
import urllib2

queue_url           = os.environ['SQS_QUEUE_URL']
aws_region          = os.environ['AWS_REGION']
config_tablename    = os.environ['CONFIG_TABLE']

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

def remove_config_entry(key):
    
    ddb.delete_item(
        TableName=config_tablename,
        Key={
            'key': {
                'S' : key
            }
        }
    )

def put_config_entry(key, value):
    
    ddb.put_item(
        TableName=config_tablename,
        Item={
            'key': {
                'S' : key,
            },
            'value' : {
                'S' : value
            }
        }
    )

def get_config_entry(key):
    
    value = None
    
    try:
        item = ddb.get_item(
            TableName=config_tablename,
            Key={
                'key': {
                    'S' : key
                }
            },
            AttributesToGet=[
                'value'
            ],
            ConsistentRead=False
        )
    
        print item
        value = item['Item']['value']['S']
    except Exception as e:
        print "Unable to get item from config table: %s" % key
    
    return value
        

def register_instance_ips(instance_ids):
    
    # Describe instances to find IP of each instance
    desc_inst_resp = ec2.describe_instances(
        InstanceIds=instance_ids
    )
    
    reservations = desc_inst_resp['Reservations']
    for reservation in reservations:
        for instance in reservation['Instances']:
            put_config_entry(instance['InstanceId'], instance['PrivateIpAddress'])


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


def get_cluster_state():
    
    cmd = "redis-cli CLUSTER INFO"
    process = subprocess.Popen([ cmd ], stdout=subprocess.PIPE, shell=True) 
    out, err = process.communicate()
    lines = out.strip().split('\n')
    
    for line in lines:
        key   = line.split(':')[0]
        value = line.split(':')[1]
        
        if key == 'cluster_state':
            return value.strip()


def get_cluster_nodes():
    cmd = "redis-cli CLUSTER NODES"
    process = subprocess.Popen([ cmd ], stdout=subprocess.PIPE, shell=True) 
    out, err = process.communicate()
    lines = out.strip().split('\n')
    
    node_ipport_dict = dict()
    ipport_node_dict = dict()
    
    for line in lines:
        fields = line.split(' ')
        
        node_dict = dict()
        node_dict['id']         = fields[0]
        node_dict['ipport']     = fields[1]
        node_dict['flags']      = fields[2]
        node_dict['master']     = fields[3]
        node_dict['link-state'] = fields[7]
        
        slots = ""
        if len(fields) > 8:
            for i in range(8, len(fields)):
                slots = slots + " " + fields[i]
        
        node_dict['slots'] = slots.strip()
        
        node_ipport_dict[node_dict['id']]     = node_dict
        ipport_node_dict[node_dict['ipport']] = node_dict
    
    return {'node_ipport_dict': node_ipport_dict, 'ipport_node_dict': ipport_node_dict}

def create_cluster(asg_name, num_replicas):
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
        cmd = cmd + instance_ip + ":" + str(REDIS_PORT) + " "
    cmd = cmd + " </tmp/yes"
    
    print("Command: " + cmd)
    
    # Create 'yes' file
    f = open('/tmp/yes', 'w+')
    f.write('yes')
    f.close()
    
    # Run the redis-trib.rb create command
    subprocess.check_call(cmd, shell=True)
    
    # Register cluster nodes
    nodes = get_cluster_nodes()
    node_ipport_dict = nodes['node_ipport_dict']
    for key, value in node_ipport_dict.iteritems():
        put_config_entry(value['id'], value['ipport'])

def handle_instance_launch(instance_id, lc_token):
    print "Handling new instance: %s" % instance_id
    if instance_id == my_instance_id:
        sys.exit(1)
    
    instance_ids = []
    instance_ids.append(instance_id)
    
    register_instance_ips(instance_ids)
    
    new_instance_ip = get_instance_ips(instance_ids)[0]
    my_instance_ip = get_instance_ips([my_instance_id])[0]
    
    cmd = "redis-cli -h %s -p %s CLUSTER MEET %s %s" % (new_instance_ip, REDIS_PORT, my_instance_ip, REDIS_PORT)
    subprocess.check_call(cmd, shell=True)
    
    # Register cluster node
    nodes = get_cluster_nodes()
    node_ipport_dict = nodes['node_ipport_dict']
    ipport = "%s:%d" % (new_instance_ip, REDIS_PORT)
    for key, value in node_ipport_dict.iteritems():
        if value['ipport'] == ipport:
            put_config_entry(value['id'], value['ipport'])
            break

def handle_instance_termination(instance_id, lc_token):
    print "Handling termination for instance: %s" % instance_id
    
    if instance_id == my_instance_id:
        sys.exit(1)
    
    instance_ip = get_config_entry(instance_id)
    ipport = "%s:%d" % (instance_ip, REDIS_PORT)
    
    cluster_nodes = get_cluster_nodes()
    node_ipport_dict = cluster_nodes['node_ipport_dict']
    ipport_node_dict = cluster_nodes['ipport_node_dict']
        
    # Check if instance is master or slave
    node = ipport_node_dict[ipport]
    
    # Find unused 'master' instance
    unused_master = None
    unused_master_ip = None
    unused_master_port = None
    
    for key, value in node_ipport_dict.iteritems():
        if 'master' in value['flags'] and not 'fail' in value['flags'] and not value['slots'].strip():
            unused_master = value
            unused_master_ipport = unused_master['ipport']
            unused_master_ip = unused_master_ipport.split(':')[0]
            unused_master_port = unused_master_ipport.split(':')[1]
            break
    
    if not unused_master:
        print "No unused masters available"
        sys.exit(1)
    
    if 'master' in node['flags']:
        master_node_id = node['id']
        
        # If master and has slots
        if node['slots']:
            print 'Node is master.  Finding a slave to takeover master role'
        
            slave_to_promote = None
        
            for key, value in node_ipport_dict.iteritems():
                if (
                        'slave' in value['flags']
                        and 'fail' not in value['flags']
                        and master_node_id in value['master'] 
                        and 'connected' in value['link-state']
                    ):
                    slave_to_promote = value
                    break
        
            if slave_to_promote:
                slave_ip = slave_to_promote['ipport'].split(':')[0]
                slave_port = slave_to_promote['ipport'].split(':')[1]
                cmd = "redis-cli -h %s -p %s CLUSTER FAILOVER TAKEOVER" % (slave_ip, slave_port)
                subprocess.check_call(cmd, shell=True)
                print "Sent CLUSTER FAILOVER TAKEOVER to %s" % slave_to_promote['ipport']
                
                # Make unused master slave of promoted slave
                make_unused_master_into_slave(unused_master_ip, unused_master_port, slave_to_promote['id'])
                
            else:
                print "Unable to find any slaves to promote so making unused master take over"
                
                cmd = "redis-cli -h %s -p %s CLUSTER FAILOVER TAKEOVER" % (unused_master_ip, unused_master_port)
                subprocess.check_call(cmd, shell=True)
                print "Sent CLUSTER FAILOVER TAKEOVER to %s" % unused_master_ip
        
        else: # Master without slots
            # Find master with least number of slaves
            print "Node is master without slots.  Finding master with fewest slaves"
            
            master_slave_dict = dict()
            for key, value in node_ipport_dict.iteritems():
                if '-' not in value['master'] and 'fail' not in value['flags']:
                    slaves = master_slave_dict.get(value['master'])
                    if slaves == None:
                        slaves = []
                        master_slave_dict[value['master']] = slaves
                    slaves.append(value['id'])
                    print "Added slave: %s" % value['id']
            
            master_with_fewest_slaves = None
            fewest_slave_count = 0
            for key, value in master_slave_dict.iteritems():
                if not master_with_fewest_slaves:
                    master_with_fewest_slaves = key
                    fewest_slave_count = len(value)
                elif len(value) < fewest_slave_count:
                    master_with_fewest_slaves = key
                    fewest_slave_count = len(value)
            
            if not master_with_fewest_slaves:
                print "Unable to find master with fewest slaves"
                sys.exit(1)
            
            print "Master %s has the fewest slaves (%d)" % (master_with_fewest_slaves, fewest_slave_count)
            
            # Assign unused master as a slave to the master with the fewest slaves
            make_unused_master_into_slave(unused_master_ip, unused_master_port, master_with_fewest_slaves)                

                
    if 'slave' in node['flags'] and unused_master:        
        print "Instance is a slave and we have unused master"
        
        master = ipport_node_dict[ipport]['master']
        
        # Tell unused master to replicate from terminating slave's master
        make_unused_master_into_slave(unused_master_ip, unused_master_port, master)
    
    # Delete the node & instance id from the config
    remove_config_entry(node['id'])
    remove_config_entry(instance_id)
    

def do_cleanup():
    
    if get_cluster_state() != 'ok':
        print "Cluster is not in an OK state.  Cannot process cleanup"
        return
    
    # Get cluster nodes
    cluster_nodes = get_cluster_nodes()
    node_ipport_dict = cluster_nodes['node_ipport_dict']
    
    # Check all nodes in the fail state
    for key, value in node_ipport_dict.iteritems():
        if 'fail' in value['flags']:
            node = value
            
            # Check if node exists in the DB
            ipport = get_config_entry(node['id'])
            
            if ipport == None:
                print "Going to forget node %s" % node['id']
                cmd = "redis-cli CLUSTER FORGET %s" % node['id']
            
            try:
                subprocess.check_output(cmd, shell=True)
            except Exception as e:
                print ("Caught exception while running CLUSTER FORGET: %s" % cmd)
    
def make_unused_master_into_slave(unused_master_ip, unused_master_port, master):
    cmd = "redis-cli -h %s -p %s CLUSTER REPLICATE %s" % (unused_master_ip, unused_master_port, master)
    print cmd
    subprocess.check_call(cmd, shell=True)
    print "Sent CLUSTER REPLICATE to %s" % unused_master_ip

def complete_lc_action(lc_hook, asg_name, lc_token, result, instance_id):
    asg.complete_lifecycle_action(
        LifecycleHookName=lc_hook,
        AutoScalingGroupName=asg_name,
        LifecycleActionToken=lc_token,
        LifecycleActionResult=result,
        InstanceId=instance_id
    )
    print "Completed lifecycle action"


def handle_message(message):
    body = json.loads(message['Body'].encode("ascii"))
        
    event         = body.get('Event')
    asg_name      = body.get('AutoScalingGroupName')
    instance_id   = body.get('EC2InstanceId')
    lc_token      = body.get('LifecycleActionToken')
    lc_hook       = body.get('LifecycleHookName')
    lc_transition = body.get('LifecycleTransition')
    
    num_replicas  = body.get('NumReplicas')
    
    if event == 'CLUSTER_FORGET':
        node_id = body.get('node_id')
        forget_node(node_id)
        delete_message(message)
    
    if event == 'autoscaling:TEST_NOTIFICATION':
        print("Removing test notification")
        delete_message(message)
        return
    
    if event == 'CLUSTER_CREATE':
        print("Received CLUSTER_CREATE")
        create_cluster(asg_name, num_replicas)
        delete_message(message)
    
    if lc_transition == 'autoscaling:EC2_INSTANCE_TERMINATING':
        print("terminated")
        handle_instance_termination(instance_id, lc_token)
        delete_message(message)
        complete_lc_action(lc_hook, asg_name, lc_token, 'CONTINUE', instance_id)
        
    
    if lc_transition == 'autoscaling:EC2_INSTANCE_LAUNCHING':
        print("launching")
        handle_instance_launch(instance_id, lc_token)
        delete_message(message)
        complete_lc_action(lc_hook, asg_name, lc_token, 'CONTINUE', instance_id)

def is_leader():
    
    # Get ASG name from tag
    desc_inst_resp = ec2.describe_instances(
        InstanceIds=[my_instance_id]
    )
    instance = desc_inst_resp['Reservations'][0]['Instances'][0]
    
    asg_name = None
    
    tags = instance['Tags']
    for tag in tags:
        if tag['Key'] == 'aws:autoscaling:groupName':
            asg_name = tag['Value']
    
    instances = None
    if asg_name:
        desc_asg_resp = asg.describe_auto_scaling_groups(
            AutoScalingGroupNames=[
                asg_name
            ]
        )
        asg_group = desc_asg_resp['AutoScalingGroups'][0]
        instances = asg_group['Instances']
    
    if instances:
        
        # Get all instance IDS
        instance_ids = []
        for instance in instances:
            instance_ids.append(instance['InstanceId'])
        
        desc_inst_resp = ec2.describe_instances(
            InstanceIds=instance_ids
        )
        
        # Find oldest instance
        oldest_instance_id = None
        oldest_instance_launch_time = None
        
        for reservation in desc_inst_resp['Reservations']:
            for instance in reservation['Instances']:
                if oldest_instance_id == None:
                    oldest_instance_id = instance['InstanceId']
                    oldest_instance_launch_time = instance['LaunchTime']
                else:
                    if instance['LaunchTime'] < oldest_instance_launch_time:
                        oldest_instance_id = instance['InstanceId']
                        oldest_instance_launch_time = instance['LaunchTime']
                    if instance['LaunchTime'] == oldest_instance_launch_time:
                        if instance['InstanceId'] < oldest_instance_id:
                            oldest_instance_id = instance['InstanceId']
        
        print "Leader instance is: %s" % oldest_instance_id
                
        if my_instance_id == oldest_instance_id:
            return True
    
    return False

def main():
    
    print("Performing cleanup")
    try:
        do_cleanup()
    except Exception as e:
        print("Caught exception while doing cleanup: ", e)
    
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
