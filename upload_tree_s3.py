#! /usr/bin/env python
# upload local tree to s3: individual file; a directory; or a directory tree
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
import     sqsmsg
import     awscontext

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
upload_count = 0
skip_count = 0

#max size in bytes before uploading in parts. between 1 and 5 GB recommended
MAX_SIZE = 20 * 1000 * 1000
#size of parts when uploading in parts
PART_SIZE = 6 * 1000 * 1000

def uploadFile(s3_a, bucketname_a, path_a, file_a):
    global skip_count
    global upload_count
    srcpath = os.path.join(path_a, file_a)
    destpath = os.path.join(path_a, file_a)
    # remove leading "/" in destpath
    if destpath[0] == "/":
        destpath = destpath[1:len(destpath)]
    # if only uploading changed check
    if changed:
        key = destpath
        obj = s3.Bucket(bucketname_a).Object(key)
        try:
            # compare last modifed times in s3 (utc) vs local (timestamp)
            # first convert s3 time to local and without a timezone attr
            s3_dm_utc = obj.last_modified
            s3_dm_local = s3_dm_utc.astimezone(tz.tzlocal())
            s3_dm_notz = s3_dm_local.replace(tzinfo=None)
            # now get local time (as a timestamp) and convert to datetime
            lmt_ts = os.path.getmtime(srcpath)
            local_dm_notz = datetime.datetime.fromtimestamp(lmt_ts)
            # compare
            if local_dm_notz > s3_dm_notz:
                upload = True
            else:
                upload = False
        except:
            upload = True   # local file not in s3
    else:
        upload = True

    if upload:
        pInfo('Uploading ' + srcpath +' to s3://' + bucketname_a + '/' + destpath)
        s3.Bucket(bucketname_a).upload_file(srcpath, destpath,
                                            ExtraArgs={'StorageClass': 'STANDARD_IA',
                                                       'ServerSideEncryption': 'AES256'})
        upload_count += 1
    else:
        skip_count += 1
        pDebug("Skipping older " + srcpath)

def findfile(fn_a, filter_a):
    found = False
    for ft in filter_a:
        if fnmatch.fnmatch(fn_a,ft):
            found = True
            break
    return found

def finddir(dir_a, filter_a):
    found = False
    dl = dir_a.split('/')
    for ft in filter_a:
        if ft in dl:
            found = True
            break
    return found

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
    print( '\tSource file: ' + srcfile)
    print( '\tSource dir: ' + srcdir)
    print ('\tInclude filter: ' + str(include))
    print ('\tExclude filter: ' + str(exclude))
    print ('\tDirectories include filter: ' + str(incdir))
    print ('\tDirectories exclude filter: ' + str(exdir))
    print ('\tRecursive copy: ' + str(recursive))
    print ('\tCopy changed or new files: ' + str(changed))
    print( '\tLog file of update: ' + logfile)
    print( '\tS3 Bucket: ' + bucketname)
    print( '\tAWS cli profile: ' + profile)
    print( '\tSQS message: ' + message)
    print( '\tSQS type of message: ' + typemessage)
    print( '\tSQS full encoded message: ' + sqsmsg)
    print( '\tSQS URL: ' + url)
    print( '\tNo messages to SQS when update complete: ' + str(nomessaging))
    print( '\tDebug: ' + str(debug))
    print( '\tTest: ' + str(test))
    tmsg=time.asctime()
    print( '\tTime: ' + tmsg)
# defaults
defLogfile = './update_to_s3.log'
defMsg = 'updated s3'
defTypeMsg = 's3change'
defAwsCtx = 'default'

# parse input
parser = ArgumentParser( description = "script to copy local directory tree to s3 and send an sqs msg" )
parser.add_argument( "-C", "--ctxfile",
                     help = "Contexts json file [default: awscontext.json]" )
parser.add_argument( "-p", "--profile",
                     help = "Profile for aws credentials [default: based on awsctx]" )
parser.add_argument( "-a", "--awsctx", default = defAwsCtx,
                     help = "Contexts json file [default: " + defAwsCtx + "]" )
parser.add_argument( "-r", "--recursive", action="store_true", default = False,
                     help = "Recursively copy tree folder and subfolders [default: False]" )
parser.add_argument( "-c", "--changed", action="store_false", default = True,
                     help = "Only upload new or changed (by date modified) [default: True]" )
parser.add_argument( "-i", "--include",
                     help = "Filter the files (comma delimited) to include [default: no filtering]" )
parser.add_argument( "-e", "--exclude",
                     help = "Filter the files (comma delimited) to exlude [default: no filtering]" )
parser.add_argument( "-I", "--incdir",
                     help = "Filter the directories (comma delimited) to include [default: no filtering]" )
parser.add_argument( "-E", "--exdir",
                     help = "Filter the directories (comma delimited) to exlude [default: no filtering]" )
parser.add_argument( "-L", "--links", action="store_true", default = False,
                     help = "Follow symbolic links [default: False]" )
parser.add_argument( "-m", "--message", default = defMsg,
                     help = "message to send to sqs [default: " + defMsg + "]" )
parser.add_argument( "-t", "--typemessage", default = defTypeMsg,
                     help = "type of message to send to sqs [default: " + defTypeMsg + "]" )
parser.add_argument( "-N", "--nomessaging",action="store_true", default = False,
                     help = "No messages when update completes [default: False]" )
parser.add_argument( "-l", "--logfile", default = defLogfile,
                     help = "log file of sync to s3 [default: " + defLogfile + "]" )
parser.add_argument( "-s", "--source",
                     help = "source of folder or file in tree to copy [default: cwd's tree]" )
parser.add_argument( "-D", "--Debug", action="store_true", default = False,
                     help = "Turn on debug output [default: False]" )
parser.add_argument( "-S", "--summary", action="store_true", default = False,
                     help = "Print summary prior to executing [default: False]" )
parser.add_argument( "-T", "--test", action="store_true", default = False,
                     help = "Test without upload or sending message [default: False]" )
parser.add_argument( "--version", action="store_true", default = False,
                     help = "Print version of " + __file__ )
args = parser.parse_args()
# set result of arg parse_args
ctxfile = args.ctxfile
awsctx = args.awsctx
profile = args.profile
message = args.message
typemessage = args.typemessage
logfile = args.logfile
debug = args.Debug
source = args.source
summary = args.summary
include = args.include
exclude = args.exclude
recursive = args.recursive
nomessaging = args.nomessaging
changed = args.changed
incdir = args.incdir
exdir = args.exdir
links = args.links
test = args.test

# create the awscontext object
allctx = awscontext.awscontext(ctx_file = ctxfile, verbose = debug)

bucketname = allctx.getbucketname(awsctx)
if bucketname == None:
    pError('Bucket name not found in ' + awsctx)
    sys.exit(2)
url = allctx.getsqsurl(awsctx)
if url == None:
    pError('SQS url not found in ' + awsctx)
    sys.exit(2)
if profile == None:
    profile = allctx.getprofile(awsctx)
    if profile == None:
        pError('Profile not found in ' + awsctx)
        sys.exit(2)

# version
if args.version:
    print(__file__ + " version: " + version)
    sys.exit()
# process source (could be path, filename, or both)
if source == None:
    srcdir = os.getenv('PWD')
    srcfile = ''
else:
    if not os.path.exists(source):
        pError('Source ' + source + ' does not exist')
        sys.exit(2)
    # check if it's a single file
    if not os.path.isdir(source):
        srcfile = os.path.basename(source)
        srcdir = os.path.abspath(os.path.dirname(source))
        # single file is not recursive
        recursive = False
    else:
        srcfile = ''
        srcdir = os.path.abspath(source)
# create aws session with s3 with appropriate credentials
if profile == None:
    profile = 'default'
# sqs message
sqsmsg = sqsmsg.encode(message, typemsg = typemessage)
if test:
    debug = True
    nomessaging = True
    summary = True
# create list of inc/ex
delim = ','
if include != None:
    include = include.replace(' ','').split(delim)
if exclude != None:
    exclude = exclude.replace(' ','').split(delim)
# if including or excluding directories then it's recursive
if incdir != None or exdir != None:
    recursive = True
    if incdir != None:
        incdir = incdir.replace(' ','').split(delim)
    if exdir != None:
        exdir = exdir.replace(' ','').split(delim)

# summary
if summary:
    Summary("Summary of " + __file__)
    if not test:
        sys.exit()

# Create boto3 session - any clients created from this session will use credentials
# from the [dev] section of ~/.aws/credentials.
session = boto3.Session(profile_name=profile)
s3 = session.resource('s3')

if srcfile != '':
    # process single file
    if test:
        pDebug("Testing: would upload "  + srcdir + "/" + srcfile)
    else:
        pInfo("Uploading single file " + srcdir + "/" + srcfile)
        uploadFile(s3, bucketname, srcdir, srcfile)
else:
    # process directory
    if recursive:
        pInfo("Uploading " + srcdir + " recursively ...")
        print("\tBuilding list ...")
    else:
        pInfo("Uploading files in " + srcdir + "...")
    alldirs = {}
    for (source, dirname, filename) in os.walk(srcdir, followlinks = links):
        alldirs[source] = filename
        if not recursive:
            break
    print("\tIterating over " + str(len(alldirs)) + " directory/directories ...")
    for dir, files in alldirs.iteritems():
        #print("dir: " + dir)
        #print("\tfiles: " + str(files))
        if incdir != None:
            if not finddir(dir, incdir):
                continue
        elif exdir != None:
            if finddir(dir, exdir):
                continue
        if len(files) > 0:
            for filename in files:
                # check for filters
                if include != None:
                    if not findfile(filename, include):
                        continue
                elif exclude != None:
                    if findfile(filename,exclude):
                        continue
                if test:
                    pDebug('Testing: would upload ' + dir + '/' + filename + ' to s3://' + bucketname)
                else:
                    uploadFile(s3, bucketname, dir, filename)
if nomessaging:
    pInfo("Message not sent to SQS")
else:
    # send a message to sqs
    pInfo("Sending SQS message")
    sqs=session.client("sqs")
    pDebug('Sending SQS:\nURL: ' + url + '\nmessage to encode: ' + message)
    response=sqs.send_message(QueueUrl=url,MessageBody=sqsmsg)

pInfo("Upload completed.\n\tUpload count: " + str(upload_count) + "\n\tSkip count: " + str(skip_count))
