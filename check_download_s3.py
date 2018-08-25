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
    print( '\tS3 Bucket: ' + bucket)
    print( '\tEBS Root Folder: ' + rootfolder)
    print( '\tLog file of download: ' + logfile)
    print( '\tMessage log: ' + messagelog)
    print( '\tSQS URL: ' + url)
    print( '\tWait time for sqs msg: ' + str(waittime))
    print( '\tPurge message queue after receiving message: ' + str(purgequeue))
    print( '\tDebug: ' + str(debug))
    print( '\taws cli profile: ' + profile)
    print( '\taws cli command:\n' + awscmd)
    tmsg=time.asctime()
    print( '\tTime: ' + tmsg)
# poll sqs
def pollSQS(url_a, waittime_a):
    # get the sqs client
    sqs = boto3.client("sqs")
    # loop until a message is received
    receivedmsg = False;
    msgKey = "Messages"
    theMsg = ""
    while not receivedmsg:
        msg = sqs.receive_message(QueueUrl=url_a,WaitTimeSeconds=waittime_a)
        if msgKey in msg.keys():
            theMsg = msg[msgKey][0]['Body']
            rHandle = msg[msgKey][0]['ReceiptHandle']
            receivedmsg = True
            # delete the msg
            result = sqs.delete_message(QueueUrl=url_a,
                                        ReceiptHandle=rHandle)
    return theMsg
# defaults
defSqsUrl = 'https://sqs.us-west-2.amazonaws.com/988956399400/s3_test'
defS3Bucket = 'projects-pearson'
defSourcefolder = ''
defRootfolder = "/nfs_ebs"
defS3Command = 's3 sync --sse AES256 --storage-class=STANDARD_IA'
defLogfile = './download_from_s3.log'
defMsglog = './received_messages.log'
# parse input
parser = ArgumentParser( description = "script to copy local files to s3 and send an sqs msg" )
parser.add_argument( "-u", "--url", default = defSqsUrl,
                     help = "url of sqs [default: " + defSqsUrl + "]" )
parser.add_argument( "-P", "--purgequeue", action="store_true", default = False,
                     help = "Purge sqs queue after receiving first message [default: false]" )
parser.add_argument( "-b", "--bucket", default = defS3Bucket,
                     help = "Source s3 bucket [default: " + defS3Bucket + "]" )
parser.add_argument( "-w", "--waittime", type = int, default = 20,
                     help = "Wait time for checking messages (secs) [default: 20 (max)]" )
parser.add_argument( "-m", "--messagelog", default = defMsglog,
                     help = "message log [default: " + defMsglog + "]" )
parser.add_argument( "-l", "--logfile", default = defLogfile,
                     help = "log file of download from s3 [default: " + defLogfile + "]" )
parser.add_argument( "-r", "--rootfolder", default = defRootfolder,
                     help = "EBS root folder [default: " + defRootfolder + "]" )
parser.add_argument( "-s", "--sourcefolder", default = defSourcefolder,
                     help = "S3 source folder [default: " + defSourcefolder + "]" )
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
messagelog = args.messagelog
purgequeue = args.purgequeue
bucket = args.bucket
logfile = args.logfile
debug = args.Debug
summary = args.summary
profile = args.profile
waittime = args.waittime
rootfolder = args.rootfolder
sourcefolder = args.sourcefolder
# version
if args.version:
    print(__file__ + " version: " + version)
    sys.exit()
# check ebs root folder
if not os.path.isdir(rootfolder):
    pError("EBS root folder " + rootfolder + " does not exist" )
    sys.exit(2)
# aws command
if profile == None:
    profile = 'default'
source = 's3://' + bucket + sourcefolder
dest = rootfolder
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
# open message log
mlog = open (messagelog, 'a+')
# loop for ever to process messages
pInfo("forever loop to get sqs message ...")
while True:
    # poll for message
    msg = pollSQS(url, waittime)
    # execute awscmd
    pInfo("Message found: " + msg)
    # redirect stdout to logile
    pInfo("Executing s3 sync ...:")
    pDebug("aws cli cmd:\n" + awscmd)
    sout = sys.stdout
    serr = sys.stderr
    flog = open ( logfile )
    mlog.write(msg+"\r\n")
    sys.stderr = sys.stdout = flog
    # create subprocess to execute
    process = subprocess.Popen(awscmd, stdout=sys.stdout, stderr=sys.stderr, shell=True)
    status = process.wait()
    # redirect stdout back
    sys.stdout = sout
    sys.stderr = serr
    sys.stdout.flush()
    sys.stderr.flush()
    flog.close()
    # check status
    if status:
        pError("aws cli command failed: " + str(status) + ". See " + logfile + " for details.")
        sys.exit(2)
    # check if purge msg queue
    if purgequeue:
        pInfo("Purging message queue: " + url)
        # get the sqs client
        sqs = boto3.client("sqs")
        # purge
        sqs.purge_queue(QueueUrl=url)
    pInfo("Sync complete; waiting for next message")
