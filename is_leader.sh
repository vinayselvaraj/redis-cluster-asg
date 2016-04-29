#!/bin/bash

die() { status=$1; shift; echo "FATAL: $*"; exit $status; }
THIS_INSTANCE_ID="`curl http://169.254.169.254/latest/meta-data/instance-id || die \"curl instance-id has failed: $?\"`"
LEADER_INSTANCE_ID="`aws --region us-east-1 ec2 describe-instances --filters "Name=tag:aws:autoscaling:groupName,Values=$ASG_NAME" "Name=instance-state-name,Values=running" | grep -o '\"i-[0-9a-f]\\+\"' | grep -o '[^\"]\\+' | sort | head -n 1 || die \"could not list autoscaling group instances: $?\"`"

echo $LEADER_INSTANCE_ID

if [ "$THIS_INSTANCE_ID" == "$LEADER_INSTANCE_ID" ]
then
  exit 0
else
  exit 1
fi
