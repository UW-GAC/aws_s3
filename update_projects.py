#! /usr/bin/env python
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
    print( '\tSource folder: ' + source)
    print ('\tInclude filter: ' + str(include))
    print ('\tExclude filter: ' + str(exclude))
    print ('\tRecursive copy (via sync): ' + str(recursive))
    print( '\tLog file of update: ' + logfile)
    print( '\tS3 Bucket: ' + bucket)
    print( '\tSQS message: ' + message)
    print( '\tSQS URL: ' + url)
    print( '\tNo messages to SQS when update complete: ' + str(nomessaging))
    print( '\tDebug: ' + str(debug))
    print( '\taws cli profile: ' + profile)
    print( '\taws cli command:\n' + awscmd)
    tmsg=time.asctime()
    print( '\tTime: ' + tmsg)

# defaults
defSqsUrl = 'https://sqs.us-west-2.amazonaws.com/988956399400/s3_test'
defS3Bucket = 'projects-pearson'
defLogfile = './update_to_s3.log'
# parse input
parser = ArgumentParser( description = "script to copy local files to s3 and send an sqs msg" )
parser.add_argument( "-u", "--url", default = defSqsUrl,
                     help = "url of sqs [default: " + defSqsUrl + "]" )
parser.add_argument( "-r", "--recursive", action="store_true", default = False,
                     help = "Recursively copy subdirecties (via sync) [default: False]" )
parser.add_argument( "-i", "--include",
                     help = "Filter the files to include [default: no filtering]" )
parser.add_argument( "-e", "--exclude",
                     help = "Filter the files to exlude [default: no filtering]" )
parser.add_argument( "-m", "--message",
                     help = "message sent to sqs [default: <time> update <user>]" )
parser.add_argument( "-N", "--nomessaging",action="store_true", default = False,
                     help = "No messages when update completes [default: False]" )
parser.add_argument( "-b", "--bucket", default = defS3Bucket,
                     help = "destination s3 bucket [default: " + defS3Bucket + "]" )
parser.add_argument( "-l", "--logfile", default = defLogfile,
                     help = "log file of sync to s3 [default: " + defLogfile + "]" )
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
logfile = args.logfile
debug = args.Debug
source = args.source
summary = args.summary
profile = args.profile
include = args.include
exclude = args.exclude
recursive = args.recursive
nomessaging = args.nomessaging

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
# check for recursive (sync) or not (cp)
if recursive:
    cc = 'sync'
else:
    cc = 'cp'
# aws command w/o filters
if profile == None:
    profile = 'default'
defS3Command = 's3 --sse AES256 --storage-class=STANDARD_IA'
dest = 's3://' + bucket + source
awscmd = 'aws --profile ' + profile + ' s3 ' + cc + \
   ' --sse AES256 --storage-class=STANDARD_IA ' + \
   ' ' + source + ' ' + dest
# check for filters
if include != None:
    awscmd = awscmd + ' --exclude "*" --include "' + include + '"'
if exclude != None:
    awscmd = awscmd + ' --exclude "' + exclude + '"'
# sqs message
if message == None:
    user = getpass.getuser()
    message = time.asctime() + " update " + dest + " by " + user
# summary
if summary:
    Summary("Summary of " + __file__)
    sys.exit()
# execute awscmd
pInfo("Executing aws cli to sync s3 bucket with command:\n" + awscmd)
# redirect stdout to logile
sys.stdout.flush()
sys.stderr.flush()
sout = sys.stdout
serr = sys.stderr
flog = open ( logfile, 'w' )
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
if sendmessage:
    # send a message to sqs
    pInfo("Sending SQS message")
    sqs=boto3.client("sqs")
    pDebug('Sending SQS:\nURL: ' + url + '\nmessage: ' + message)
    response=sqs.send_message(QueueUrl=url,MessageBody=message)
else:
    pInfo("Message not sent to SQS")
pInfo("Sync complete.")
