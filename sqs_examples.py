import boto3
client = boto3.client('sqs')
s3msg = client.receive_message(
   QueueUrl='https:#sqs.us-west-2.amazonaws.com/988956399400/s3_test',
   AttributeNames=['All'],
   MessageAttributeNames=['All'])
rh2='AQEBruNeRqMRs14Je39r3Pp9noyl6SUM/cXXQNFEf8B4VAt1q25/k2HCAv3XSZ+zZvEyOqfaB5Q85GtWma0iI0aFwEtA/IGiIATkNinCJ67b8RmcNHjUCmxQEcfUfJ4mZ2tbFYjABTfq/pctpt3jwuQJxPF+6kVUFEBppA+9nJ3ggz6L8MLortoXgUxqhDg1tw592jUIY+ZVjZ3+88ZzIkhVM1X67q3Fm+VavJOhYh4+91KNWSox0W85fwNEtobGu53GcRuQkQ9Siu6YCBF6G8bvQbqRSbA/YLPzhdiNpCJXkcstxmYGzSk6HJ305pF34ZXgktFEYWNgLu/1aOi9g9HtB5AvEYjBLFA494XqZCkX5jKyi44NEgX91CEKBvxuZTyKQegh8mywoJ/kCJMrRJ2TVA=='
qurl='https:#sqs.us-west-2.amazonaws.com/988956399400/s3_test'
res=client.delete_message(QueueUrl=qurl,ReceiptHandle=rh2)

res=client.purge_queue(QueueUrl=qurl)
type(s3msg['Messages'][0]['Body'])
body=ast.literal_eval(s3msg['Messages'][0]['Body'])
body['object']
body['Records'][0]
body['Records'][0]['s3']
body['Records'][0]['s3']['object']
s3msg['Messages'][0]['ReceptHandle']
s3msg['Messages'][0]['ReceiptHandle']
# get history from running python interactively
import readline
for i in range(readline.get_current_history_length()):
    print readline.get_history_item(i + 1)

# send message
import boto3
client=boto3.client("sqs")
qurl='https://sqs.us-west-2.amazonaws.com/988956399400/s3_test'
response=client.send_message(MessageBody='update')
response=client.send_message(QueueUrl=qurl,MessageBody='update')
response

# get message
s3msg = client.receive_message(QueueUrl=qurl)
s3msg['Messages'][0]['Body']
# delete the message
res=client.delete_message(QueueUrl=qurl,ReceiptHandle=s3msg['Messages'][0]['ReceiptHandle'])

# use message attributes
mattribute={'MType': {'StringValue': 'Test', 'DataType': 'String'}}
mbody='msg with attribute'

response=client.send_message(QueueUrl=qurl,MessageBody=mbody,MessageAttributes=mattribute)
# purge
res=client.purge_queue(QueueUrl=qurl)
#=============================================
# 1. get date modified from s3 and local; compare
# 2. list folders (keys) in s3 bucket
#
# 1. get date modified
import boto3
import os
import datetime
from dateutil import tz
session = boto3.Session()
s3 = session.resource('s3')
bucket_name = 'projects-pearson'
key='projects/topmed/analysts/kuraisa/analysis_pipeline/install_packages.R'
obj=s3.Bucket(bucket_name).Object(key)
s3_dm=obj.last_modified   # tzone is utc

os.path.getmtime('/projects/topmed/analysts/kuraisa/analysis_pipeline/install_packages.R')
timestamp=os.path.getmtime('/projects/topmed/analysts/kuraisa/analysis_pipeline/install_packages.R')
l_dm=datetime.datetime.fromtimestamp(ldm)
s3_ldm =s3_dm.astimezone(tz.tzlocal())
s3_ldm_notz=s3_ldm.replace(tzinfo=None)
if ldt > s3_ldm_notz:
   print('yep')
else:
   print('nope')
# 2. list folders in bucket
objs = s3.Bucket(bucket_name).objects.all()
keys = []
for obj in objs:
    keys.append(obj.key)
# find methods in objs
[method_name for method_name in dir(objs) if callable(getattr(objs, method_name))]

#=============================================
# 1. list objects with prefix filtering which requires paginators
import boto3
session = boto3.Session()
s3 = session.resource('s3')
client = s3.meta.client
bucket_name = 'projects-pearson'
# prefix must not leading '/' and must have a trailing '/' to list objects
# down the tree.
# NOTE: list_objects will limit return objects to 1000 (check 'IsTruncated')
x=client.list_objects(Bucket=bucket_name,Prefix='projects/topmed/analysts/kuraisa/')
# look at the keys and if it's a valid prefix, 'Contents' key will be there
x.keys()    # should see 'Contents'
x['Contents'][0]   # first item in contents list
x['IsTrunctated']
# use paginators
keys2 = []
lo_por = client.get_paginator('list_objects')
p_iter = lo_por.paginate(Bucket=bucket_name)
for o in p_iter:
    nc = len(o['Contents'])
    for c in range(nc):
        keys2.append(o['Contents'][c]['Key'])
# w/ filtering
keys2 = []
parameters = {'Bucket': bucket_name,
              'Prefix': 'projects/topmed/analysts/kuraisa/analysis_pipeline'}
p_iter = lo_por.paginate(**parameters)
for o in p_iter:
    nc = len(o['Contents'])
    for c in range(nc):
        keys2.append(o['Contents'][c]['Key'])
