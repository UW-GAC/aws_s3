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
import     awscontext
import     signal

try:
    import boto3
except ImportError:
    print (__file__ + ": python boto3 not supported.")
    sys.exit(1)

def keyboardInterruptHandler(signal, frame):
    print("KeyboardInterrupt (ID: {}) has been caught. Exiting program ...".format(signal))
    exit(0)

signal.signal(signal.SIGINT, keyboardInterruptHandler)

# init globals
version='2.0'
msgErrPrefix='>>> Error: '
msgInfoPrefix='>>> Info: '
debugPrefix='>>> Debug: '

#max size in bytes before uploading in parts. between 1 and 5 GB recommended
MAX_SIZE = 20 * 1000 * 1000
#size of parts when uploading in parts
PART_SIZE = 6 * 1000 * 1000

def uploadFile(s3_a, bucketname_a, srcpath_a, destpath_a, test_a):
    # if only uploading changed check
    if changed:
        key = destpath_a
        obj = s3.Bucket(bucketname_a).Object(key)
        try:
            # compare last modifed times in s3 (utc) vs local (timestamp)
            # first convert s3 time to local and without a timezone attr
            s3_dm_utc = obj.last_modified
            s3_dm_local = s3_dm_utc.astimezone(tz.tzlocal())
            s3_dm_notz = s3_dm_local.replace(tzinfo=None)
            # now get local time (as a timestamp) and convert to datetime
            lmt_ts = os.path.getmtime(srcpath_a)
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
        if os.path.islink(srcpath_a):
            upload = False
            pInfo('Skipping the linked file ' + srcpath_a)
        elif not test_a:
            try:
                pDebug('Uploading ' + srcpath_a +' to s3://' + bucketname_a + '/' + destpath_a)
                s3.Bucket(bucketname_a).upload_file(srcpath_a, destpath_a,
                                                    ExtraArgs={'StorageClass': 'STANDARD_IA',
                                                               'ServerSideEncryption': 'AES256'})
            except Exception as e:
                pError('S3 bucket (' + bucketname_a + ') upload error: ' + str(e))
                upload = False

    return upload

def findfile(fn_a, filter_a):
    found = False
    for ft in filter_a:
        if fnmatch.fnmatch(fn_a,ft):
            found = True
            break
    return found

def finddir(dir_a, filter_a):
    found = False
    ld = len(dir_a)
    filter_a
    for ft in filter_a:
        ft = "/" + ft
        ft2 = ft + "/"
        lft = len(ft)
        if ld >= lft:
            if dir_a[ld-lft:ld] == ft or ft2 in dir_a:
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
    print( '\t========  Copy Info ==========')
    print( '\tSource file: ' + srcfile)
    print( '\tSource dir: ' + srcdir)
    print ('\tInclude filter: ' + str(include))
    print ('\tExclude filter: ' + str(exclude))
    print ('\tDirectories include filter: ' + str(incdir))
    print ('\tDirectories exclude filter: ' + str(exdir))
    print ('\tRecursive copy: ' + str(recursive))
    print ('\tCopy changed or new files: ' + str(changed))
    print( '\t========  Configuration Info ==========')
    print( '\tContext cfg file argument: ' + str(ctxfile))
    print( '\taws context: ' + str(awsctx))
    print( '\tS3 Bucket: ' + bucketname)
    print( '\tAWS credentials profile: ' + profile)
    print( '\t========  General Info ==========')
    print( '\tVersion: ' + version)
    print( '\tDebug: ' + str(debug))
    print( '\tTest: ' + str(test))
    tmsg=time.asctime()
    print( '\tTime: ' + tmsg)
# defaults
defLogfile = './update_to_s3.log'
defMsg = 'updated s3'
defTypeMsg = 's3change'
defAwsCtx = 'uw'
# begin time
tbegin=time.asctime()

# parse input
parser = ArgumentParser( description = "script to copy local directory tree to s3" )
parser.add_argument( "-C", "--ctxfile",
                     help = "Contexts json file [default: awscontext.json]" )
parser.add_argument( "-p", "--profile",
                     help = "Profile for aws credentials [default: based on awsctx in ctxfile]" )
parser.add_argument( "-b", "--bucketname",
                     help = "S3 bucket name [default: based on awsctx in ctxfile]" )
parser.add_argument( "-a", "--awsctx", default = defAwsCtx,
                     help = "aws contex in ctxfile [default: " + defAwsCtx + "]")
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
parser.add_argument( "-s", "--source",
                     help = "source of folder or file in tree to copy [default: cwd's tree]" )
parser.add_argument( "-D", "--Debug", action="store_true", default = False,
                     help = "Turn on debug output [default: False]" )
parser.add_argument( "-S", "--summary", action="store_false", default = True,
                     help = "Print summary prior to executing [default: False]" )
parser.add_argument( "-T", "--test", action="store_true", default = False,
                     help = "Test without upload [default: False]" )
parser.add_argument( "--version", action="store_true", default = False,
                     help = "Print version of " + __file__ )
args = parser.parse_args()
# set result of arg parse_args
ctxfile = args.ctxfile
awsctx = args.awsctx
profile = args.profile
bucketname = args.bucketname
debug = args.Debug
source = args.source
summary = args.summary
include = args.include
exclude = args.exclude
recursive = args.recursive
changed = args.changed
incdir = args.incdir
exdir = args.exdir
links = args.links
test = args.test

# create the awscontext object
allctx = awscontext.awscontext(ctx_file = ctxfile, verbose = debug)

# check for valid awsctx
if allctx.getctx(awsctx) == None:
    pError('Error: awsctx ' + awsctx + ' not found')
    sys.exit(2)

if bucketname == None:
    bucketname = allctx.getbucketname(awsctx)
    if bucketname == None:
        pError('Bucket name not found in ' + awsctx)
        sys.exit(2)

if profile == None:
    profile = allctx.getprofile(awsctx)

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
# set the aws profile
if profile == None:
    profile = 'default'
if test:
    debug = True
    summary = True
# create list of inc/ex
delim = ','
pd = '/'
if include != None:
    # include cannot include a path (/)
    if pd in include:
        pError("Include files cannot include a path (" + include + ")")
        sys.exit(2)
    include = include.replace(' ','').split(delim)
if exclude != None:
    # exlude cannot include a path (/)
    if pd in exclude:
        pError("Exclude files cannot include a path (" + exclude + ")")
        sys.exit(2)
    exclude = exclude.replace(' ','').split(delim)
# if including or excluding directories then it's recursive
if incdir != None or exdir != None:
    recursive = True
    if incdir != None:
        if pd in incdir:
            pError("Include dirs cannot include a path (" + incdir + ")")
            sys.exit(2)
        incdir = incdir.replace(' ','').split(delim)
    if exdir != None:
        if pd in exdir:
            pError("Exclude dirs cannot include a path (" + exdir + ")")
            sys.exit(2)
        exdir = exdir.replace(' ','').split(delim)

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
if srcfile != '':
    # process single file
    skip_count = 0
    upload_count = 0
    srcpath = srcdir + "/" + srcfile
    if test:
        pDebug("Testing: would upload "  + srcpath)
    else:
        pInfo("Uploading single file " + srcpath)
        destpath = srcpath
        # remove leading "/" in destpath
        if destpath[0] == "/":
            destpath = destpath[1:len(destpath)]
        if not uploadFile(s3, bucketname, srcpath, destpath, test):
            skip_count = skip_count + 1
        else:
            upload_count = upload_count + 1
else:
    # process directory
    if recursive:
        pInfo("Uploading " + srcdir + " recursively ...")
        print("\tBuilding list ...")
    else:
        pInfo("Uploading files in " + srcdir + "...")
    alldirs = {}
    totfiles = 0
    for (source, dirname, filename) in os.walk(srcdir, followlinks = links):
        alldirs[source] = filename
        totfiles = totfiles + len(filename)
        if not recursive:
            break
    upload_count = 0
    skip_count = 0
    print("\tIterating over " + str(len(alldirs)) + " directory/directories ...")
    for dir, files in alldirs.iteritems():
        #print("dir: " + dir)
        #print("\tfiles: " + str(files))
        if incdir != None:
            if not finddir(dir, incdir):
                skip_count = skip_count + len(files)
                continue
        elif exdir != None:
            if finddir(dir, exdir):
                skip_count = skip_count + len(files)
                continue
        if len(files) > 0:
            for filename in files:
                # check for filters
                if include != None:
                    if not findfile(filename, include):
                        skip_count += 1
                        continue
                elif exclude != None:
                    if findfile(filename,exclude):
                        skip_count += 1
                        continue
                srcpath = os.path.join(dir, filename)
                destpath = srcpath
                # remove leading "/" in destpath
                if destpath[0] == "/":
                    destpath = destpath[1:len(destpath)]
                upfile = uploadFile(s3, bucketname, srcpath, destpath, test)
                if upfile:
                    upload_count += 1
                    if not test:
                        msghdr = 'Uploaded '
                    else:
                        msghdr = "Testing: Would have uploaded "
                    pInfo(msghdr + '(' + str(upload_count+skip_count) + '/' + str(totfiles) + ')' +
                          srcpath + ' to s3://' + bucketname)
                else:
                    skip_count += 1
                    pDebug('Skipped ' + srcpath)

pInfo("Upload completed.\n\tBegin time: " + tbegin +
      "\n\tUpload count: " + str(upload_count) +
      "\n\tSkip count: " + str(skip_count))
