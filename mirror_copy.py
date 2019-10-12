#! /usr/bin/env python
# Mirror copy from uw pearson /projects/... to an NFS volume on aws
import     time
import     csv
import     sys
import     os
import     subprocess
from       argparse import ArgumentParser
from       datetime import datetime, timedelta
import     getpass
import     datetime
from       dateutil import tz
import     signal
import     glob


def keyboardInterruptHandler(signal, frame):
    print("KeyboardInterrupt (ID: {}) has been caught. Exiting program ...".format(signal))
    exit(0)

signal.signal(signal.SIGINT, keyboardInterruptHandler)

# init globals
version='1.0'
msgErrPrefix='>>> Error: '
msgInfoPrefix='>>> Info: '
debugPrefix='>>> Debug: '

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
    print( '\tSource: ' + source)
    print( '\tRemote host: ' + remotehost)
    print( '\tRemote user: ' + remoteuser)
    print( '\tRoot directory: ' + rootdir)
    print( '\tSSH key: ' + privatekey)
    print ('\tInclude pattern: ' + str(include))
    print ('\tExclude pattern: ' + str(exclude))
    print( '\tVersion: ' + version)
    print( '\tDebug: ' + str(debug))
    print( '\tExecute: ' + str(execute))
    print( '\tTest: ' + str(test))
    tmsg=time.asctime()
    print( '\tTime: ' + tmsg)
# defaults
defRemoteHost  = '52.10.169.19'
defPvtKey = 'topmed_admin.pem'
defRemoteUser = getpass.getuser()
defRootDir = "/projects/"
# begin time
tbegin=time.asctime()

# parse input
parser = ArgumentParser( description = "Mirror copy (via rsync) source under /projects/... to NFS volume on AWS" )
parser.add_argument( "source", nargs = '+', help = "source of files to copy" )
parser.add_argument( "-p", "--privatekey", default = defPvtKey,
                     help = "SSH private key [default: " + defPvtKey + "]")
parser.add_argument( "-H", "--remotehost", default = defRemoteHost,
                     help = "Remote host [default: " + defRemoteHost + "]")
parser.add_argument( "-u", "--remoteuser", default = defRemoteUser,
                     help = "Remote user [default: " + defRemoteUser + "]")
parser.add_argument( "-R", "--rootdir", default = defRootDir,
                     help = "Root directory to mirror [default: " + defRootDir + "]")
parser.add_argument( "-i", "--include",
                     help = "Patterns (comma delimited) of file names to include (overrides -e) [default: no including]" )
parser.add_argument( "-e", "--exclude",
                     help = "Patterns (comma delimited) of file names to exclude [default: no excluding]" )
parser.add_argument( "-D", "--Debug", action="store_true", default = False,
                     help = "Turn on debug output [default: False]" )
parser.add_argument( "-S", "--summary", action="store_false", default = True,
                     help = "Print summary prior to executing [default: False]" )
parser.add_argument( "-T", "--test", action="store_true", default = False,
                     help = "Execute rsync without copying [default: False]" )
parser.add_argument( "-N", "--noexecute", action="store_true", default = False,
                     help = "Do not execute the rsync command [default: False]" )
parser.add_argument( "--version", action="store_true", default = False,
                     help = "Print version of " + __file__ )
args = parser.parse_args()
# set result of arg parse_args
if len(args.source) > 1:
    pError('More than one commandline arguments (enclose * in quotes)')
    sys.exit(2)
source = args.source[0]
privatekey = args.privatekey
remotehost = args.remotehost
remoteuser = args.remoteuser
rootdir = args.rootdir
include = args.include
exclude = args.exclude
debug = args.Debug
summary = args.summary
test = args.test
execute = not args.noexecute

# version
if args.version:
    print(__file__ + " version: " + version)
    sys.exit()

# check rootdir (must have leading and trailing "/")
slash = "/"
if rootdir[0] != slash or rootdir[len(rootdir)-1] != "/":
    pError("Root directory " + rootdir + " is not valid - requires leading and trailing '/'")
    sys.exit(2)

# process source
srcAbsPath = os.path.abspath(source)
srcDir = os.path.dirname(srcAbsPath)
srcBasename = os.path.basename(srcAbsPath)
# source can be:
#  1. a single file
#  2. directory
#  3. file pattern (e.g., *.log)
if source.find('*') or source.find('?'):
    nofiles = len(glob.glob(source))
    if nofiles == 0:
        pError("No files from source " + source)
        sys.exit(2)
else:
    if not os.path.isdir(srcBasename) and not os.path.isfile(srcBasename):
        pError(source + " is not a file nor a directory")
        sys.exit(2)

# check basename

# check for valid root dir
if srcAbsPath.find(rootdir) != 0:
    pError("Required root directory (" + rootdir + ") is not the root directory in source: " + srcAbsPath)
    sys.exit(2)

if summary:
    Summary("Upload Tree Summary")

# build thr rsync command
rscommand = "rsync -av -O --no-perms --update --chmod=Fg+w "
# ssh
rscommand += '"-e ssh -i ~/.ssh/' + privatekey + '" '
# --exclude or --include options
delim = ','
pd = '/'
if include != None:
    # include cannot include a path (/)
    if pd in include:
        pError("Include patterns cannot include a path (" + include + ")")
        sys.exit(2)
    include = include.replace(' ','').split(delim)
    for inc in include:
        rscommand += "--include=" + inc + " "
if exclude != None:
    # exlude cannot include a path (/)
    if pd in exclude:
        pError("Exclude patterns cannot include a path (" + exclude + ")")
        sys.exit(2)
    exclude = exclude.replace(' ','').split(delim)
    for exc in exclude:
        rscommand += "--exclude=" + exc + " "

# remote stuff and direcory stuff
# from the absolute source path (e.g., /projects/topmed/...) we need to enable folders
# below root /projects by creating /projects/./ ...
rsource = srcAbsPath.replace(rootdir,rootdir+"./")

# source
rscommand += "--relative " + rsource + " "
rscommand += remoteuser + "@" + remotehost + ":" + rootdir + " "

if test:
    rscommand += "-n"
pInfo('Executing command: \n' + rscommand)
if not execute:
    sys.exit(0)
# redirect stdout to logile
process = subprocess.Popen(rscommand, stdout=sys.stdout, stderr=sys.stderr, shell=True)
status = process.wait()
if status:
    pError( "Executing rsync failed (" + str(status) + ")" )
    sys.exit(2)
else:
    pInfo( "Executing rsync completed without errors.")
