#! /usr/bin/env python
import     time
import     csv
import     sys
import     os
import     subprocess
from       argparse import ArgumentParser
from       datetime import datetime, timedelta
import     getpass
import     fnmatch
import     datetime
from       dateutil import tz
import     distutils.dir_util
import     sqsmsg

try:
    import boto3
except ImportError:
    print (__file__ + ": python boto3 not supported.")
    sys.exit(1)

# init globals
version='1.0'
msgErrPrefix='>>> Error: '
msgInfoPrefix='>>> Info: '
debugPrefix='>>> Debug: '
logPrefix='>>> Log: '
download_count = 0
skip_count = 0
error_count = 0

def downloadFile(s3_a, bucketname_a, key_a, mtime_a, destfolder_a, dlog_a):
    global skip_count
    global download_count
    # create dst folder name based on key and destfolder_a
    if destfolder_a == None:
        dpath = '/' + key
    else:
        dpath = destfolder_a + '/' + key
    # check if destination (dir + file) exists
    if os.path.exists(dpath):
        dfile = True
    # else check if the dir exists
    else:
        dfile = False
        destdir = os.path.dirname(dpath)
        if not os.path.exists(destdir):
            distutils.dir_util.mkpath(destdir)
    # if dfile exists, download if only changed
    if dfile:
        if changed:
            s3_dm_utc = mtime_a
            s3_dm_local = s3_dm_utc.astimezone(tz.tzlocal())
            s3_dm_notz = s3_dm_local.replace(tzinfo=None)
            # now get local time (as a timestamp) and convert to datetime
            #pDebug('get modified time from ' + dpath)
            lmt_ts = os.path.getmtime(dpath)
            local_dm_notz = datetime.datetime.fromtimestamp(lmt_ts)
            # compare
            if local_dm_notz > s3_dm_notz:
                download = False
            else:
                download = True
        else:
            download = True
    else:
        download = True
    # download
    if download:
        LogInfo(dlog_a, 'Downloading s3://' + bucketname_a + '/' + key + ' to ' + dpath)
        try:
            s3.Bucket(bucketname_a).download_file(key, dpath)
            download_count += 1
        except:
            LogInfo(dlog_a, 'Download failed; perhaps bucket has changed since the list of keys were created.')
            error_count += 1
    else:
        skip_count += 1
        LogInfo(dlog_a, 'Skipping older s3://' + bucketname_a + '/' + key)

def LogInfo(dlog_a, msg_a):
    tmsg=time.asctime()
    dlog_a.write(logPrefix+tmsg+": "+msg_a+"\n")

def ServiceLog(msg_a):
    print(__file__ + ": " + msg_a)
    sys.stdout.flush()

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
    print( '\tAWS profile: ' + profile)
    print( '\tS3 Bucket: ' + bucketname)
    print( '\tDestination Root Folder: ' + destfolder)
    print( '\tMessage log: ' + messagelog)
    print( '\tDetail log: ' + logfile)
    print( '\tAWS cli profile: ' + profile)
    print( '\tSQS URL: ' + url)
    print( '\tWait time for sqs msg: ' + str(waittime))
    print( '\tPurge message queue after receiving message: ' + str(purgequeue))
    print( '\tDebug: ' + str(debug))
    tmsg=time.asctime()
    print( '\tTime: ' + tmsg)
# poll sqs
def pollSQS(sqs_a, url_a, waittime_a):
    try:
        # loop until a message is received
        receivedmsg = False;
        msgKey = "Messages"
        theMsg = ""
        while not receivedmsg:
            msg = sqs_a.receive_message(QueueUrl=url_a,WaitTimeSeconds=waittime_a)
            if msgKey in msg.keys():
                theMsg = msg[msgKey][0]['Body']     # msg is encoded
                rHandle = msg[msgKey][0]['ReceiptHandle']
                receivedmsg = True
        return (theMsg,rHandle)
    except KeyboardInterrupt:
        ServiceLog('Keyboard interrupt; exiting')
        sys.exit()
    except Exception as e:
	ServiceLog('Exception while polling: ' + str(e))
        sys.exit(2)
# defaults
defSqsUrl = 'https://sqs.us-west-2.amazonaws.com/988956399400/s3_test'
defS3Bucket = 'projects-pearson'
defRootfolder = "/nfs_ebs"
defMsglog = '/tmp/syncs3_messages.log'
defLogfile = '/tmp/syncs3_details.log'
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
                     help = "Detail log file [default: " + defLogfile + "]" )
parser.add_argument( "-d", "--destfolder",
                     help = "Destination root folder [default: same root folder as in bucket (e.g., /projects)]" )
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
bucketname = args.bucket
debug = args.Debug
summary = args.summary
profile = args.profile
waittime = args.waittime
destfolder = args.destfolder
logfile = args.logfile
changed = True
dall = True
summary = True
sourcefolder = ''
# version
if args.version:
    print(__file__ + " version: " + version)
    sys.exit()
# aws config
if profile == None:
    profile = 'default'
# check destfolder
if destfolder != None:
    destfolder = os.path.abspath(destfolder)

# summary
if summary:
    Summary("Summary of " + __file__)
# Create boto3 session - any clients created from this session will use credentials
# from the [dev] section of ~/.aws/credentials.
session = boto3.Session(profile_name=profile)
s3 = session.resource('s3')
# open message log
mlog = open (messagelog, 'a+', 1)
# open detail log file
dlog = open (logfile, 'w', 1)
# get the sqs client
sqs = session.client("sqs")

# loop for ever to process messages
ServiceLog("forever loop to get sqs message ...")
while True:
    # poll for message
    mh = pollSQS(sqs, url, waittime)
    msge = mh[0]
    rHandle = mh[1]
    # decode msg into dictionary
    try:
        msgd = sqsmsg.decode(msge)
    except:
        LogInfo(dlog, 'Invalid format of message; it will be deleted.')
        sqs.delete_message(QueueUrl=url,
                           ReceiptHandle=rHandle)
        continue
    msg = msgd['msg']
    msgtype = msgd['type']
    msgdate = msgd['date']
    msgbywhom = msgd['bywhom']
    # if the type is s3change, sync s3
    if msgtype == 's3change':
        ServiceLog('Message found: ' + msge)
        ServiceLog('Syncing s3 to destfolder ...')
        keys = []
        mtimes = []
        LogInfo(dlog, 'Getting all folders/data names in S3 bucket ' + bucketname)
        objs = s3.Bucket(bucketname).objects.all()
        for obj in objs:
            keys.append(obj.key)
            mtimes.append(obj.last_modified)
        # iterate over keys and download each
        noKeys = len(keys)
        for i in range(noKeys):
            key = keys[i]
            filename = os.path.basename(key)
            mtime = mtimes[i]
            downloadFile(s3, bucketname, key, mtime, destfolder, dlog)
        # write the log and delete the message
        mlog.write(msge + '\n')
        ServiceLog('Sync complete - files downloaded/skip/error: ' + str(download_count) +
                   '/' + str(skip_count) + '/' + str(error_count))
        # check if purge msg queue
        if purgequeue:
            ServiceInfo("Purging message queue: " + url)
            # get the sqs client
            sqs = boto3.client("sqs")
            # purge
            sqs.purge_queue(QueueUrl=url)
    else:
        ServiceLog('No-action message found: ' + msge)
    # delete the message
    result = sqs.delete_message(QueueUrl=url,
                                ReceiptHandle=rHandle)

    ServiceLog("Waiting for next message")
