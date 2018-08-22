#! /usr/bin/env python
try:
    import boto3
except ImportError:
    print ("python boto3 not supported.")
    sys.exit(1)

import     time
import     csv
import     sys
import     os.path
import     os
import     subprocess
from       argparse import ArgumentParser
from       datetime import datetime, timedelta
import     requests
import     getpass

# init globals
version='1.0'
msgErrPrefix='>>> Error: '
msgInfoPrefix='>>> Info: '
debugPrefix='>>> Debug: '

def pInfo(msg):
    tmsg=time.asctime()
    print msgInfoPrefix+tmsg+": "+msg

def pError(msg):
    tmsg=time.asctime()
    print msgErrPrefix+tmsg+": "+msg

def pDebug(msg):
    if debug:
        tmsg=time.asctime()
        print debugPrefix+tmsg+": "+msg
def Summary(hdr):
    print(hdr)
    print( '\tVersion: ' + version)
    print( '\tSource folder: ' + source)
    print( '\tS3 Bucket: ' + bucket)
    print( '\tSQS message: ' + message)
    print( '\tSQS URL: ' + url)
    if debug:
        print( '\tDebug: True' )
    else:
        print( '\tDebug: False' )
    print( '\taws cli profile: ' + profile)
    print( '\taws cli command:\n' + awscmd)
    tmsg=time.asctime()
    print( '\tTime: ' + tmsg)

# defaults
defSqsUrl = 'https://sqs.us-west-2.amazonaws.com/988956399400/s3_test'
defS3Bucket = 'projects-pearson'
defS3Command = 's3 sync --only-show-errors --sse AES256 --storage-class=STANDARD_IA'
# parse input
parser = ArgumentParser( description = "script to copy local files to s3 and send an sqs msg" )
parser.add_argument( "-u", "--url", default = defSqsUrl,
                     help = "url of sqs [default: " + defSqsUrl + "]" )
parser.add_argument( "-m", "--message",
                     help = "message sent to sqs [default: 'update']" )
parser.add_argument( "-b", "--bucket", default = defS3Bucket,
                     help = "destination s3 bucket [default: " + defS3Bucket + "]" )
parser.add_argument( "-s", "--source",
                     help = "source directory to recursively copy [default: cwd]" )
parser.add_argument( "-p", "--profile",
                     help = "aws cli profile [default: default]" )
parser.add_argument( "-D", "--Debug", action="store_true", default = False,
                     help = "Turn on debug output [default: False]" )
parser.add_argument( "-S", "--summary", action="store_true", default = False,
                     help = "Print summary prior to executing [default: False]" )
parser.add_argument( "--version", action="store_true", default = False,
                     help = "Print version of " + __file__ )
args = parser.parse_args()
# set result of arg parse_args
url = args.url
message = args.message
bucket = args.bucket
debug = args.Debug
source = args.source
summary = args.summary
profile = args.profile
# version
if args.version:
    print(__file__ + " version: " + version)
    sys.exit()
# source directory
if source == None:
    source = os.getenv('PWD')
source = os.path.abspath(source)
# check if /projects is root
reqRoot = "projects"
if source.split("/")[1] != reqRoot:
    pError("Source folder " + source + " is not under " + reqRoot)
    sys.exit(2)
# aws command
if profile == None:
    profile = 'default'
dest = 's3://' + bucket + source
awscmd = 'aws --profile ' + profile + ' ' + defS3Command + \
   ' ' + source + ' ' + dest
# sqs message
if message == None:
    user = getpass.getuser()
    message = time.asctime() + " update " + dest + " by " + user
# summary
if summary:
    Summary("Summary of " + __file__)
    sys.exit()
# execute awscmd
pInfo("Executing aws cli to sync s3 bucket ...")
if debug:
    pDebug("aws cli cmd:\n" + awscmd)
process = subprocess.Popen(awscmd, shell=True, stdout=subprocess.PIPE)
status = process.wait()
pipe = process.stdout
msg = pipe.readline()
if status:
    pError("aws cli command failed: " + msg )
    sys.exit(2)
# send a message to sqs
pInfo("Sending SQS message")
sqs=boto3.client("sqs")
if debug:
    pDebug('Sending SQS:\nURL: ' + url + '\nmessage: ' + message)
response=sqs.send_message(QueueUrl=url,MessageBody=message)
pInfo("Sync complete.")
