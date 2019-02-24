#! /usr/bin/env python
# download tree in s3: individual file; a directory; or a directory tree
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
import     awscontext

try:
    import boto3
except ImportError:
    print (__file__ + ": python boto3 not supported.")
    sys.exit(1)

# init globals
version='2.0'
msgErrPrefix='>>> Error: '
msgInfoPrefix='>>> Info: '
debugPrefix='>>> Debug: '
download_count = 0
skip_count = 0
error_count = 0

def downloadFile(s3_a, bucketname_a, key_a, mtime_a, destfolder_a):
    global skip_count
    global download_count
    global error_count
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
        pInfo('Downloading s3://' + bucketname_a + '/' + key + ' to ' + dpath)
        try:
            s3.Bucket(bucketname_a).download_file(key, dpath)
            download_count += 1
        except Exception as e:
            pError('Bucket.download error: ' + str(e))
            error_count += 1
    else:
        skip_count += 1
        pDebug('Skipping older s3://' + bucketname_a + '/' + key)

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
    print( '\tSource folder: ' + srcfolder)
    if destfolder == None:
        print( '\tDestination root folder: same as in S3 bucket')
    else:
        print( '\tDestination root folder: ' + destfolder)
    print ('\tOnly changed files: ' + str(changed))
    print ('\tInclude filter: ' + str(include))
    print ('\tExclude filter: ' + str(exclude))
    print( '\tLog file of update: ' + logfile)
    print( '\tDebug: ' + str(debug))
    print( '\tTest: ' + str(test))
    tmsg=time.asctime()
    print( '\tTime: ' + tmsg)

# defaults
defLogfile = './download_from_s3.log'
defAwsCtx = 'default'
defRootfolder = '/projects'
# parse input
parser = ArgumentParser( description = "script to download a tree/folder/file from s3 to a local directory" )
parser.add_argument( "-C", "--ctxfile",
                     help = "Contexts json file [default: awscontext.json]" )
parser.add_argument( "-p", "--profile",
                     help = "Profile for aws credentials [default: based on awsctx]" )
parser.add_argument( "-b", "--bucketname",
                     help = "S3 bucket name [default: based on awsctx]" )
parser.add_argument( "-a", "--awsctx", default = defAwsCtx,
                     help = "Contexts json file [default: " + defAwsCtx + "]" )
parser.add_argument( "-l", "--logfile", default = defLogfile,
                     help = "Detail log file [default: " + defLogfile + "]" )
parser.add_argument( "-d", "--destfolder",
                     help = "Destination root folder [default: " + defRootfolder + "]" )
parser.add_argument( "-i", "--include",
                     help = "Filter the files to include [default: no filtering]" )
parser.add_argument( "-e", "--exclude",
                     help = "Filter the files to exlude [default: no filtering]" )
parser.add_argument( "-s", "--source",
                     help = "source folder (or key) in s3 bucket to copy [default: all folders in bucket ]" )

parser.add_argument( "-T", "--test", action="store_true", default = False,
                     help = "Test without downloading [default: False]" )
parser.add_argument( "-D", "--Debug", action="store_true", default = False,
                     help = "Turn on debug output [default: False]" )
parser.add_argument( "-S", "--summary", action="store_true", default = False,
                     help = "Print summary prior to executing [default: False]" )
parser.add_argument( "--version", action="store_true", default = False,
                     help = "Print version of " + __file__ )
args = parser.parse_args()
# set result of arg parse_args
ctxfile = args.ctxfile
awsctx = args.awsctx
profile = args.profile
bucketname = args.bucketname
debug = args.Debug
summary = args.summary
destfolder = args.destfolder
logfile = args.logfile
test = args.test

source = args.source
summary = args.summary
profile = args.profile
include = args.include
exclude = args.exclude
destfolder = args.destfolder
changed = True

# create the awscontext object
allctx = awscontext.awscontext(ctx_file = ctxfile, verbose = debug)

if bucketname == None:
    bucketname = allctx.getbucketname(awsctx)
    if bucketname == None:
        pError('Bucket name not found in ' + awsctx)
        sys.exit(2)

if profile == None:
    profile = allctx.getprofile(awsctx)
    if profile == None:
        pError('Profile not found in ' + awsctx)
        sys.exit(2)

# check destfolder
if destfolder != None:
    destfolder = os.path.abspath(destfolder)
# set src folder
if source == None:
    srcfolder = ''
    dall = True
else:
    srcfolder = source
    if srcfolder[0] != '/':
        srcfolder = '/' + srcfolder
    dall = False
# version
if args.version:
    print(__file__ + " version: " + version)
    sys.exit()

# summary
if summary:
    Summary("Summary of " + __file__)

# Create boto3 session - any clients created from this session will use credentials
# from the [dev] section of ~/.aws/credentials.
try:
    session = boto3.Session(profile_name=profile)
    s3 = session.resource('s3')
except Exception as e:
    pError('boto3 session or client exception ' + str(e))
    sys.exit(2)
# get the keys (folders/paths) that have data
# if download all, we use objects.all
keys = []
mtimes = []
if dall:
    pInfo('Getting all folders/data names in S3 bucket ' + bucketname)
    objs = s3.Bucket(bucketname).objects.all()
    for obj in objs:
        keys.append(obj.key)
        mtimes.append(obj.last_modified)
# else we must use a paginator to get the filtered objects (in case > 1000 objects)
else:
    pInfo('Getting keys in S3 bucket ' + bucketname + ' from prefix: ' + srcfolder)
    client = s3.meta.client
    lo_por = client.get_paginator('list_objects')
    # filter - remove leading '/'
    if srcfolder[0] == '/':
        srcfolder = srcfolder[1:len(srcfolder)]
    parameters = {'Bucket': bucketname,
                  'Prefix': srcfolder}
    p_iter = lo_por.paginate(**parameters)
    for o in p_iter:
        nc = len(o['Contents'])
        for c in range(nc):
            keys.append(o['Contents'][c]['Key'])
            mtimes.append(o['Contents'][c]['LastModified'])
# iterate over keys
noKeys = len(keys)
pInfo('Iterating over the keys (' + str(noKeys) + ') in S3 bucket ' + bucketname)
for i in range(noKeys):
    key = keys[i]
    filename = os.path.basename(key)
    mtime = mtimes[i]
    download = True
    if include != None:
        if not fnmatch.fnmatch(filename,include):
            download = False
    elif exclude != None:
        if fnmatch.fnmatch(filename,exclude):
            download = False
    if test:
        if destfolder == None:
            pInfo("Testing: would download to basefolder of S3://" + bucketname + "/" + key)
        else:
            pInfo("Testing: would download to " + destfolder + " from S3://" + bucketname + "/" + key)
    else:
        if download:
            downloadFile(s3, bucketname, key, mtime, destfolder)

pInfo("Download completed.\n\tDownload count: " + str(download_count) +
                         "\n\tSkip count: " + str(skip_count) +
                         "\n\tErro count: " + str(error_count))
