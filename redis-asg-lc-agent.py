#!/usr/bin/env python2.7
# Any code, applications, scripts, templates, proofs of concept, documentation and other items provided by AWS under
# this SOW are "AWS Content," as defined in the Agreement, and are provided for illustration purposes only. All such
# AWS Content is provided solely at the option of AWS, and is subject to the terms of the Addendum and the Agreement.
# Customer is solely responsible for using, deploying, testing, and supporting any code and applications provided by
# AWS under this SOW.

__doc__ = """
This module handles lifecycle events from AutoScaling and adds/removes nodes from a Redis cluster
"""
__author__ = "vinas@amazon.com"

import boto3
import json
import os
import subprocess
import sys
import uuid
import requests
import logging
import logging.handlers
import time

# Setup logging
handler = logging.handlers.SysLogHandler(address = '/dev/log')
logger = logging.getLogger('redis-asg-lc-agent')
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

queue_url           = os.environ['SQS_QUEUE_URL']
aws_region          = os.environ['AWS_REGION']
config_tablename    = os.environ['CONFIG_TABLE']

REDIS_PORT          = 6379

MY_INSTANCE_ID      = requests.get('http://169.254.169.254/latest/meta-data/instance-id').text

boto3.setup_default_session(region_name=aws_region)

sqs   = boto3.client('sqs')
asg   = boto3.client('autoscaling')
ec2   = boto3.client('ec2')
ddb   = boto3.client('dynamodb')

def delete_message(message):
    message_id     = message['MessageId']
    receipt_handle = message['ReceiptHandle']
    logger.info("Deleting SQS message id=%s" % message_id)
    
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

def put_config_entry(instance_id, ip_address, node_id, az):
    
    ddb.put_item(
        TableName=config_tablename,
        Item={
            'key': {
                'S' : instance_id,
            },
            'ip_address' : {
                'S' : ip_address
            },
            'node_id' : {
                'S' : node_id
            },
            'az' : {
                'S' : az
            }
        }
    )

def scan_config_table():
    result = ddb.scan(TableName=config_tablename)
    return result['Items']

def get_config_entry(instance_id):
    
    config_entry = dict()
    
    try:
        item = ddb.get_item(
            TableName=config_tablename,
            Key={
                'key': {
                    'S' : instance_id
                }
            },
            AttributesToGet=[
                'ip_address',
                'node_id'
            ],
            ConsistentRead=False
        )
    
        logger.info(item)
        
        config_entry['instance_id'] = instance_id
        config_entry['ip_address'] = item['Item']['ip_address']['S']
        config_entry['node_id'] = item['Item']['node_id']['S']
        
    except Exception as e:
        logger.exception("Unable to get item from config table: %s" % key)
    
    return config_entry
        

def get_instance_ips(instance_ids):
    
    instance_id_ip = dict()
    
    # Describe instances to find IP of each instance
    desc_inst_resp = ec2.describe_instances(
        InstanceIds=instance_ids
    )
    
    reservations = desc_inst_resp['Reservations']
    for reservation in reservations:
        for instance in reservation['Instances']:
            instance_id_ip[instance['InstanceId']] = instance['PrivateIpAddress']
    
    return instance_id_ip

def get_instance_azs(instance_ids):
    
    instance_id_az = dict()
    
    # Describe instances to find IP of each instance
    desc_inst_resp = ec2.describe_instances(
        InstanceIds=instance_ids
    )
    
    reservations = desc_inst_resp['Reservations']
    for reservation in reservations:
        for instance in reservation['Instances']:
            instance_id_az[instance['InstanceId']] = instance['Placement']['AvailabilityZone']
    
    return instance_id_az

def get_cluster_state():
    
    cmd = "redis-cli CLUSTER INFO"
    logger.info("Executing : %s" % cmd)
    process = subprocess.Popen([ cmd ], stdout=subprocess.PIPE, shell=True) 
    
    out, err = process.communicate()
    
    if err:
        logger.error(err)
        logger.info(out)
    
    lines = out.strip().split('\n')
    
    for line in lines:
        key   = line.split(':')[0]
        value = line.split(':')[1]
        
        if key == 'cluster_state':
            cluster_state = value.strip()
            logger.info("Cluster state: %s", cluster_state)
            return cluster_state


def get_cluster_nodes():
    cmd = "redis-cli CLUSTER NODES"
    logger.info("Executing : %s" % cmd)
    
    process = subprocess.Popen([ cmd ], stdout=subprocess.PIPE, shell=True) 
    out, err = process.communicate()
    
    if err:    
        logger.error(err)
        logger.info(out)
    
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
    logger.info("Creating cluster")
    
    instance_ids = []
    
    # Describe ASG to get list of instance IDS
    desc_asg_resp = asg.describe_auto_scaling_groups(
        AutoScalingGroupNames=[
            asg_name
        ]
    )
    asg_group = desc_asg_resp['AutoScalingGroups'][0]
    instances = asg_group['Instances']
    logger.info(instances)
    
    # Get instance ID of each instance
    for instance in instances:
        instance_ids.append(instance['InstanceId'])
    
    # Check if number of instances meets desired count
    desired_count = asg_group['DesiredCapacity']
    if len(instance_ids) != desired_count:
        raise Exception("Number of instances does not meet desired count")
    
    instance_id_ip = get_instance_ips(instance_ids)
    instance_ip_id = dict((v, k) for k,v in instance_id_ip.iteritems())
    
    cmd = "/usr/local/bin/redis-trib.rb create --replicas " + str(num_replicas) + " "
    for instance_ip in instance_id_ip.values():
        cmd = cmd + instance_ip + ":" + str(REDIS_PORT) + " "
    cmd = cmd + " </tmp/yes"
    
    logger.info("Command: " + cmd)
    
    # Create 'yes' file
    f = open('/tmp/yes', 'w+')
    f.write('yes')
    f.close()
    
    # Run the redis-trib.rb create command
    subprocess.check_call(cmd, shell=True)
    
    # Sleep for 60 seconds to allow gossip to happen
    time.sleep(60)
    
    # Register cluster nodes
    nodes = get_cluster_nodes()
    node_ipport_dict = nodes['node_ipport_dict']
    
    # Get instance azs
    instance_id_az = get_instance_azs(instance_ids)
    
    for key, value in node_ipport_dict.iteritems():
        instance_ip = value['ipport'].split(':')[0]
        instance_id = instance_ip_id.get(instance_ip)
        node_id     = value['id']
        logger.info("Registering instance %s,%s,%s" % (node_id, instance_id, instance_ip))
        
        put_config_entry(instance_id, instance_ip, node_id, instance_id_az[instance_id])


def handle_instance_launch(instance_id, lc_token):
    logger.info("Handling new instance: %s" % instance_id)
    if instance_id == MY_INSTANCE_ID:
        sys.exit(1)
    
    instance_ids = []
    instance_ids.append(instance_id)
    
    new_instance_ip = get_instance_ips(instance_ids).get(instance_id)
    my_instance_ip = get_instance_ips([MY_INSTANCE_ID]).get(MY_INSTANCE_ID)
    
    cmd = "redis-cli -h %s -p %s CLUSTER MEET %s %s" % (new_instance_ip, REDIS_PORT, my_instance_ip, REDIS_PORT)
    subprocess.check_call(cmd, shell=True)
    logger.info("Registered instance %s (%s)" % (instance_id, new_instance_ip))
    
    found_new_node = False
    
    # Get instance az
    instance_id_az = get_instance_azs([instance_id])
    
    # Loop up to 10 times until new found is found
    for i in range(10):
        # Register cluster node
        nodes = get_cluster_nodes()
        node_ipport_dict = nodes['node_ipport_dict']
        ipport = "%s:%d" % (new_instance_ip, REDIS_PORT)
    
        for key, value in node_ipport_dict.iteritems():
            if value['ipport'] == ipport:
                put_config_entry(instance_id, value['ipport'].split(":")[0], value['id'], instance_id_az[instance_id])
                found_new_node = True
                break
        
        if found_new_node:
            break
        else:
            logger.info("Did not find newly registered node %s (%s).  Sleeping for 30 seconds" % (instance_id, new_instance_ip))
            time.sleep(30)

def handle_instance_termination(instance_id, lc_token):
    logger.info("Handling termination for instance: %s" % instance_id)
    
    if instance_id == MY_INSTANCE_ID:
        sys.exit(1)
    
    instance_ip = get_config_entry(instance_id)['ip_address']
    ipport = "%s:%d" % (instance_ip, REDIS_PORT)
    
    cluster_nodes = get_cluster_nodes()
    node_ipport_dict = cluster_nodes['node_ipport_dict']
    ipport_node_dict = cluster_nodes['ipport_node_dict']
        
    # Check if instance is master or slave
    node = ipport_node_dict.get(ipport)
    
    # Exit if node is not found
    if not node:
        logger.info("Unable to find node (%s) in 'CLUSTER NODES'.  Treating as master with no slots", instance_id)
        node = dict()
        node['flags'] = 'master'
        node['slots'] = None
    
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
        logger.info("No unused masters available")
        sys.exit(1)
    
    # If in handshake, treat node as a master with no slots
    if 'handshake' in node['flags']:
        logger.info("Instance %s is in handshake, treating as master with no slots", instance_id)
        node['flags'] = 'master'
    
    if 'master' in node['flags']:
        
        # If master and has slots
        if node['slots']:
            logger.info('Node is master.  Finding a slave to takeover master role')
        
            master_node_id = node['id']
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
                
                failover_type = "TAKEOVER"
                if 'fail' in node['flags']:
                    failover_type = "FORCE"
                
                cmd = "redis-cli -h %s -p %s CLUSTER FAILOVER %s" % (slave_ip, slave_port, failover_type)
                subprocess.check_call(cmd, shell=True)
                logger.info("Sent CLUSTER FAILOVER %s to %s" % (failover_type, slave_to_promote['ipport']))
                
                # Make unused master slave of promoted slave
                make_slave(unused_master_ip, unused_master_port, slave_to_promote['id'])
                
            else:
                logger.info("Unable to find any slaves to promote so making unused master take over")
                
                cmd = "redis-cli -h %s -p %s CLUSTER FAILOVER TAKEOVER" % (unused_master_ip, unused_master_port)
                subprocess.check_call(cmd, shell=True)
                logger.info("Sent CLUSTER FAILOVER TAKEOVER to %s" % unused_master_ip)
        
        else: # Master without slots
            # Find master with least number of slaves
            logger.info("Node is master without slots.  Finding master with fewest slaves")
            
            master_slave_dict = dict()
            for key, value in node_ipport_dict.iteritems():
                                
                # If master, set an empty list of slaves if one does not exist
                if '-' in value['master'] and 'fail' not in value['flags'] and value['slots']:
                    slaves = master_slave_dict.get(value['id'])
                    if slaves == None:
                        slaves = []
                        master_slave_dict[value['id']] = slaves
                
                # If slave, append the the list of slaves of the master
                if '-' not in value['master'] and 'fail' not in value['flags']:
                    slaves = master_slave_dict.get(value['master'])
                    if slaves == None:
                        slaves = []
                        master_slave_dict[value['master']] = slaves
                    slaves.append(value['id'])
                    logger.info("Added slave: %s" % value['id'])
            
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
                logger.info("Unable to find master with fewest slaves")
                sys.exit(1)
            
            logger.info("Master %s has the fewest slaves (%d)" % (master_with_fewest_slaves, fewest_slave_count))
            
            # Assign unused master as a slave to the master with the fewest slaves
            make_slave(unused_master_ip, unused_master_port, master_with_fewest_slaves)                

                
    if 'slave' in node['flags'] and unused_master:        
        logger.info("Instance is a slave and we have unused master")
        
        master = ipport_node_dict[ipport]['master']
        
        # Tell unused master to replicate from terminating slave's master
        make_slave(unused_master_ip, unused_master_port, master)
    
    # Delete the config entry for the instance
    logger.info("Removing instance %s from config DB" % instance_id)
    remove_config_entry(instance_id)
    

def do_cleanup():
    
    if get_cluster_state() != 'ok':
        logger.info("Cluster is not in an OK state.  Cannot process cleanup")
        return
    
    # Get cluster nodes
    cluster_nodes = get_cluster_nodes()
    node_ipport_dict = cluster_nodes['node_ipport_dict']
    
    # Check all nodes in the fail state
    for key, value in node_ipport_dict.iteritems():
        logger.info("%s = %s" % (key, value))
        if 'fail' in value['flags']:
            node = value
            logger.info("Failed node found: %s", node['id'])
            
            # Check if node exists in the DB
            found_node_in_db = False;
            items = scan_config_table()
            
            for item in items:
                if item['node_id'] == node['id']:
                    found_node_in_db = True
                    break
                            
            if not found_node_in_db:
                logger.info("Going to forget node %s" % node['id'])
                cmd = "redis-cli CLUSTER FORGET %s" % node['id']
            
                try:
                    subprocess.check_output(cmd, shell=True)
                except Exception as e:
                    logger.info("Caught exception while running CLUSTER FORGET: %s" % cmd)
    
def make_slave(ip, port, master):
    cmd = "redis-cli -h %s -p %s CLUSTER REPLICATE %s" % (ip, port, master)
    logger.info(cmd)
    
    process = subprocess.Popen([ cmd ], stdout=subprocess.PIPE, shell=True) 
    out, err = process.communicate()
    if 'OK' in out.strip():
        logger.info("Sent CLUSTER REPLICATE to %s" % ip)
    else:
        logger.error(out)
        if(err):
            logger.error(err)
        
        raise Exception("Unable to execute: %s.  Output: %s, Error: %s", cmd, out, err)
        
    #subprocess.check_call(cmd, shell=True)
    

def complete_lc_action(lc_hook, asg_name, lc_token, result, instance_id):
    asg.complete_lifecycle_action(
        LifecycleHookName=lc_hook,
        AutoScalingGroupName=asg_name,
        LifecycleActionToken=lc_token,
        LifecycleActionResult=result,
        InstanceId=instance_id
    )
    logger.info("Completed lifecycle action for instance %s" % instance_id)

def complete_wait_handle(wait_handle_url):
    payload = {
        "Status": "SUCCESS",
        "Reason": "Redis Cluster Created",
        "UniqueId": str(uuid.uuid4()),
        "Data": "Ready"
    }
    requests.put(wait_handle_url, data = json.dumps(payload))

def handle_message(message):
    body = json.loads(message['Body'].encode("ascii"))
        
    event         = body.get('Event')
    asg_name      = body.get('AutoScalingGroupName')
    instance_id   = body.get('EC2InstanceId')
    lc_token      = body.get('LifecycleActionToken')
    lc_hook       = body.get('LifecycleHookName')
    lc_transition = body.get('LifecycleTransition')
    
    num_replicas  = body.get('NumReplicas')
    
    if event == 'autoscaling:TEST_NOTIFICATION':
        logger.info("Removing test notification")
        delete_message(message)
        return
    
    if event == 'CLUSTER_CREATE':
        logger.info("Received CLUSTER_CREATE")
        create_cluster(asg_name, num_replicas)
        wait_handle_url = body.get('WaitHandleUrl')
        complete_wait_handle(wait_handle_url)
        delete_message(message)
    
    if lc_transition == 'autoscaling:EC2_INSTANCE_TERMINATING':
        logger.info("terminated")
        handle_instance_termination(instance_id, lc_token)
        delete_message(message)
        complete_lc_action(lc_hook, asg_name, lc_token, 'CONTINUE', instance_id)
        
    
    if lc_transition == 'autoscaling:EC2_INSTANCE_LAUNCHING':
        logger.info("launching")
        handle_instance_launch(instance_id, lc_token)
        delete_message(message)
        complete_lc_action(lc_hook, asg_name, lc_token, 'CONTINUE', instance_id)

def rebalance_cluster():
    
    if get_cluster_state() != 'ok':
        logger.info("Cluster is not in an OK state.  Cannot process rebalance")
        return

    items = scan_config_table()
    ip_az_dict = dict()
    for item in items:
        ip_az_dict[item['ip_address']['S']] = item['az']['S']
    
    nodes = get_cluster_nodes()
    
    shard_az_list_dict           = dict()
    ipport_az_dict               = dict()
    shard_slave_ipport_list_dict = dict()
    
    for node_ip_port in nodes['ipport_node_dict']:
        node = nodes['ipport_node_dict'][node_ip_port]
        
        shard_id = node['id']
        if node['master'] != '-':
            shard_id = node['master']
        
        az_list = shard_az_list_dict.get(shard_id)
        if az_list == None:
            az_list = list()
            shard_az_list_dict[shard_id] = az_list
        
        az_list.append(ip_az_dict[node['ipport'].split(':')[0]])
        
        ipport_az_dict[node['ipport']] = ip_az_dict[node['ipport'].split(':')[0]]
        
        if node['master'] != '-':
            slaves = shard_slave_ipport_list_dict.get(shard_id)
            if slaves == None:
                slaves = list()
                shard_slave_ipport_list_dict[shard_id] = slaves
            slaves.append(node['ipport'])
    
    unbalanced_shards = list()
    balanced_shards   = list()
    
    for shard in shard_az_list_dict:
        logger.info("%s : %s" % (shard, shard_az_list_dict[shard]) )
        shard_az_set = set(shard_az_list_dict[shard])
        num_azs = len(shard_az_set)
        
        if num_azs == 1:
            unbalanced_shards.append(shard)
        else:
            balanced_shards.append(shard)
    
    if len(unbalanced_shards) == 0:
        logger.info("There are no unbalanced shards")
        return
    
    # Select a single unbalanced shard
    unbalanced_shard = unbalanced_shards[0]
    
    # Get the AZ the shard has instances in
    unbalanced_shard_az = shard_az_list_dict[unbalanced_shard][0]
        
    # Remove AZ from shard_az_list_dict elements
    for shard in shard_az_list_dict:
        az_list = shard_az_list_dict[shard]
        az_list = filter(lambda a: a != unbalanced_shard_az, az_list)
        shard_az_list_dict[shard] = az_list
    
    shard_with_slave = None
    
    for shard in shard_az_list_dict:
        shard_az_list = shard_az_list_dict[shard]
        num_azs = len(shard_az_list)
        if num_azs > 1:
            shard_with_slave = shard
            break
    
    if shard_with_slave is None:
        logger.info("Unable to find a shard with a slave to swap with")
        return
    
    logger.info("Selected shard %s to swap slaves with" % shard_with_slave)
    
    # Find slave in selected shard that is not in the unbalanced_shard_az
    possible_slaves_to_swap = shard_slave_ipport_list_dict[shard_with_slave]
    
    # Find slave not in the same AZ
    selected_slave_to_swap = None
    for possible_slave in possible_slaves_to_swap:
        possible_slave_az = ipport_az_dict[possible_slave]
        if possible_slave_az != unbalanced_shard_az:
            selected_slave_to_swap = possible_slave
            break
    
    logger.info("Selected slave %s to swap with" % selected_slave_to_swap)
    
    
    # Find slave in unbalanced AZ to swap out
    unbalanced_slaves = shard_slave_ipport_list_dict[unbalanced_shard]
    if len(unbalanced_slaves) == 0:
        logger.info("No slaves found for unbalanced shard")
        return
    
    unbalanced_slave = unbalanced_slaves[0]
    logger.info("Selected unbalanced slave %s" % unbalanced_slave)
    
    # Find masters
    
    selected_slave_master_id = nodes['ipport_node_dict'][selected_slave_to_swap]['master']
    unbalanced_slave_master_id = nodes['ipport_node_dict'][unbalanced_slave]['master']
    
    selected_slave_master_hostport = nodes['node_ipport_dict'][selected_slave_master_id]['ipport']
    unbalanced_slave_master_hostport = nodes['node_ipport_dict'][unbalanced_slave_master_id]['ipport']
    
    logger.info("Found masters %s %s" % (selected_slave_master_hostport, unbalanced_slave_master_hostport))
    
    # Do the swap
    make_slave(unbalanced_slave.split(':')[0], unbalanced_slave.split(':')[1], selected_slave_master_id)
    make_slave(selected_slave_to_swap.split(':')[0], selected_slave_to_swap.split(':')[1], unbalanced_slave_master_id)
    logger.info("Completed swap")
    
    

def main():
    
    logger.info("Performing rebalance")
    try:
        rebalance_cluster()
    except Exception as e:
        logger.exception("Caught exception while doing rebalance: %s", e)
    
    logger.info("Performing cleanup")
    try:
        do_cleanup()
    except Exception as e:
        logger.exception("Caught exception while doing cleanup: %s", e)
    
    logger.info("Polling for messages")
    messages = sqs.receive_message(
                                QueueUrl            = queue_url,
                                MaxNumberOfMessages = 10,
                                WaitTimeSeconds     = 20)

    if messages and messages.get('Messages'):
        try:
            for message in messages['Messages']:
                 handle_message(message)
        except Exception as e:
            logger.exception("Caught exception while processing message: %s", e)

if __name__ == "__main__":
    main()
