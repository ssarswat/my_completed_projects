#Importing necessary libraries
import sys
import json
import boto3
from kafka import KafkaConsumer
from datetime import datetime
from collections import OrderedDict


#Creating function to send alert notification
def send_alert_notification(message):
    tstamp = float(message.timestamp/1000.0)
    alert_time = datetime.utcfromtimestamp(tstamp).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    json_object = json.loads(message.value, object_pairs_hook=OrderedDict)
    json_object['alert_time'] = alert_time
    alert_message = json_object['alert_message']
    del json_object['alert_message']
    msg = alert_message.rstrip("\n")
    json_object['alert_message'] = msg
    alert = json.dumps(json_object, indent = 3)
    ARN="arn:aws:sns:us-east-1:443061449916:PatientHealthNotification"
    sns = boto3.client('sns',aws_access_key_id=key, aws_secret_access_key=sec_key, region_name="us-east-1")
    response = sns.publish(
                   TopicArn=ARN,
                   Message=alert)

#Creating Python consumer
bootstrap_servers = ['localhost:9092']
topicName = 'PatientHealthNotification'
consumer = KafkaConsumer (topicName, bootstrap_servers = bootstrap_servers,
                auto_offset_reset = 'latest')

#Reading messages from consumer
try:
            for message in consumer:
                send_alert_notification(message)    

except KeyboardInterrupt:
            sys.exit()


