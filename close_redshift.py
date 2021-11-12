"""
Author: Jarome Leslie
Date: 2021-11-10

This script closes an existing AWS Redshift cluster using the AWS Python SDK (Boto3).
"""

import json
import boto3
import configparser
import pandas as pd
import ast 

config = configparser.ConfigParser()
config.read_file(open('redshift.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")
DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

# Instantiate redshift client
redshift = boto3.client('redshift', region_name = 'us-west-2',
                     aws_access_key_id = KEY,
                     aws_secret_access_key = SECRET)

# Delete redshift cluster

redshift_response = redshift.delete_cluster(
    ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
    SkipFinalClusterSnapshot=True
)
print('Redshift cluster deleted.')

