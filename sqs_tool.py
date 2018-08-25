#! /usr/bin/env python
try:
    import boto3
except ImportError:
    print ("python boto3 not supported.")
    sys.exit(1)

import     time
import     csv
import     sys
import     os
import     subprocess
from       argparse import ArgumentParser
from       datetime import datetime, timedelta
import     getpass
# init globals
version='1.0'
msgErrPrefix='>>> Error: '
msgInfoPrefix='>>> Info: '
debugPrefix='>>> Debug: '

try:
    import boto3
except ImportError:
    print (__file__ + ": python boto3 not supported.")
    sys.exit(1)

def pInfo(msg):
    tmsg=time.asctime()
    print(msgInfoPrefix+tmsg+": "+msg)

def pError(msg):
    tmsg=time.asctime()
    print(msgErrPrefix+tmsg+": "+msg)

def pDebug(msg):
    if debug:
        tmsg=time.asctime()
        print(debugPrefix+tmsg+": "+msg)
def Summary(hdr):
    print(hdr)
    print( '\tVersion: ' + version)
    print( '\tSQS URL: ' + url)
    print( '\tPurge message queue: ' + str(purgequeue))
    print( '\tSend message: ' + str(sendmsg))
    print( '\tMessage: ' + message)
    tmsg=time.asctime()
    print( '\tTime: ' + tmsg)

# defaults
defSqsUrl = 'https://sqs.us-west-2.amazonaws.com/988956399400/s3_test'
defMsg = "Test message from " + __file__

# parse input
parser = ArgumentParser( description = "script to copy local files to s3 and send an sqs msg" )
parser.add_argument( "-u", "--url", default = defSqsUrl,
                     help = "url of sqs [default: " + defSqsUrl + "]" )
parser.add_argument( "-P", "--purgequeue", action="store_true", default = False,
                     help = "Purge sqs queue [default: False]" )
parser.add_argument( "-s", "--sendmsg", action="store_true", default = False,
                     help = "Send test message to sqs [default: False]" )
parser.add_argument( "-m", "--message", default = defMsg,
                     help = "test message to send to sqs [default: " + defMsg + "]" )
parser.add_argument( "--version", action="store_true", default = False,
                     help = "Print version of " + __file__ )
args = parser.parse_args()
# set result of arg parse_args
url = args.url
message = args.message
purgequeue = args.purgequeue
sendmsg = args.sendmsg
# version
if args.version:
    print(__file__ + " version: " + version)
    sys.exit()
    # get the sqs client
if sendmsg or purgequeue:
    sqs = boto3.client("sqs")
    # send message
    if sendmsg:
        pInfo("Sending message: " + message)
        # send it
        sqs.send_message(QueueUrl=url,MessageBody=message)

    # purge sqs
    if purgequeue:
        pInfo("Purging queue: " + url)
        sqs.purge_queue(QueueUrl=url)
else:
    parser.print_help(sys.stderr)
