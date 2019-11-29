import json
import boto3
from os import environ

region = environ['REGION']
pod = environ['POD_NAME']
email_to = environ['EMAIL_TO']
email_from = environ['EMAIL_FROM']
ses = boto3.client('ses', region_name=region)


def lambda_handler(event, context):
    print(json.dumps(event))
    # Filter C&C botnet GuardDuty findings from the analytics cluster in data pods and VPNs
    # TODO implement a more generic and configurable (but still difficult to tamper with) way of doing filters once we have more use cases
    if event['detail']['type'] == 'Backdoor:EC2/C&CActivity.B!DNS':
        for tag in event['detail']['resource']['instanceDetails']['tags']:
            if tag['key'] == 'Name' and (tag['value'].startswith('analytics-data') or tag['value'].startswith('analytics-dsit') or tag['value'].startswith('analytics-ddev') or '-vpn' in tag['value'].lower() or tag['value'].lower().endswith('-dns-relay')):
                return
    subject_line = '%s %s %s' % (pod, event['detail-type'], event['detail']['title'])
    response = ses.send_email(Destination={'ToAddresses': [email_to]},
                              Message={'Body': {'Text': {'Charset': 'UTF-8', 'Data': json.dumps(event, indent=4)}}, 'Subject': {'Charset': 'UTF-8', 'Data': subject_line}},
                              Source=email_from)