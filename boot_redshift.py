"""
Author: Jarome Leslie
Date: 2021-11-10

This script launches an AWS Redshift cluster using the AWS Python SDK (Boto3).
The cluster details and login credentials are taken from the redshift.cfg file
"""

import json
import boto3
import configparser
import pandas as pd
import psycopg2
import time

config = configparser.ConfigParser()
config.read_file(open('redshift.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')
DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH", "DWH_PORT")
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

ec2 = boto3.resource('ec2', region_name='us-west-2',
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET)

iam = boto3.client('iam', region_name='us-west-2',
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET)
redshift = boto3.client('redshift', region_name='us-west-2',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)

# Create the IAM role
try:
    print('1.1 Creating a new IAM Role')
    dwhRole = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description="Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
                            'Effect': 'Allow',
                            'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'}
        ))
except Exception as e:
    print(e)

# Attach the IAM role policy to the cluster
print('1.2 Attaching Policy')
iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")['ResponseMetadata']['HTTPStatusCode']

# Get and print the IAM role ARN
print('1.3 Get the IAM role ARN')
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
print(roleArn)

# Creating Redshift Cluster
try:
    print('1.4 Create Redshift cluster')
    response = redshift.create_cluster(
        # add parameters for hardware
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),
        # Make publicly accessible
        PubliclyAccessible=True,
        # add parameters for identifiers & credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,

        # add parameter for role (to allow s3 access)
        IamRoles=[roleArn]
    )
except Exception as e:
    print(e)

############################################
# Summary Cluster attributes
############################################

# Wait 2 minutes to allow for cluster to initialize before taking properties
print('Waiting 2 minutes for cluster setup')
time.sleep(120)


def prettyRedshiftProps(props):
    # pd.set_option('display.max_colwidth', None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus",
                  "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId', 'IamRoles']
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


myClusterProps = redshift.describe_clusters(
    ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
cluster_props = prettyRedshiftProps(myClusterProps)

# print(myClusterProps['VpcSecurityGroups'][0])

cluster_sg = myClusterProps['VpcSecurityGroups'][0]['VpcSecurityGroupId']

print(cluster_props)

cluster_props.to_csv("cluster_properties.csv")
# print(cluster_props[cluster_props.Key == 'Endpoint']['Value'][5]['Address'])

DWH_ENDPOINT = cluster_props[cluster_props.Key == 'Endpoint']['Value'][5]['Address']


# Open a TCP port to access the cluster endpoint
try:
    print('1.5 open TCP port to access cluster endpoint')
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    # print(list(vpc.security_groups.all()))
    
    # Choose the correct security group
    Sgs = list(vpc.security_groups.all())

    for sg in Sgs:

    # Open inbound rules for cluster security group
        if sg.id == cluster_sg:
            print(sg)
            sg.authorize_ingress(
                GroupName=sg.group_name,
                CidrIp='0.0.0.0/0',
                IpProtocol='TCP',
                FromPort=int(DWH_PORT),
                ToPort=int(DWH_PORT)
            )
except Exception as e:
    print(e)

# Validate Connection
print('1.6 Connect to redshift db')



conn_string = "postgresql://{}:{}@{}:{}/{}".format(
    DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_DB)
print(conn_string)

connection = psycopg2.connect(
    database=DWH_DB,
    user=DWH_DB_USER,
    password=DWH_DB_PASSWORD,
    host=DWH_ENDPOINT,
    port=int(DWH_PORT)
)

print(f"Confirm connection has zero status: {connection.closed}")
