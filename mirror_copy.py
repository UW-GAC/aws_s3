#! /usr/bin/env python3
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
import     json
from copy import deepcopy
import collections

def keyboardInterruptHandler(signal, frame):
    print("KeyboardInterrupt (ID: {}) has been caught. Exiting program ...".format(signal))
    exit(0)

signal.signal(signal.SIGINT, keyboardInterruptHandler)

# init globals
version='1.0'
msgErrPrefix='>>> Error: '
msgInfoPrefix='>>> Info: '
debugPrefix='>>> Debug: '

# cfg is read from json into nested dictionaries
# regular dictionary update loses default values below the first level
# https://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth
def update(d, u):
    ld = deepcopy(d)
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            if len(v) == 0:
                ld[k] = u[k]
            else:
                r = update(d.get(k, {}), v)
                ld[k] = r
        else:
            ld[k] = u[k]
    return ld

# config class
class Config(object):
    # constructor
    def __init__(self, cfg_file_name="mirror_cfg.json", platform="gcp", opt_cfg=None, verbose=False):
        self.verbose = verbose
        self.className = self.__class__.__name__
        self.stdCfg = cfg_file_name
        self.optCfg = opt_cfg
        # set path of cmd
        self.commandPath = os.path.dirname(os.path.abspath(sys.argv[0]))
        # get the mirror and platformCfg configuration
        self.openCfg()
        # opt cfg file
        if self.optCfg != None:
            self.openCustomCfg()
        # update the platform and platform cfg
        self.updatePlatform(platform)

    def printVerbose(self, msg):
        if self.verbose:
            print(">>> Config: " + msg)

    def openCfg(self):
        # get the standard cluster cfg
        self.stdCfg =  os.path.join(self.commandPath, self.stdCfg)
        self.printVerbose("reading std cfg file: " + self.stdCfg)

        if  not os.path.isfile(self.stdCfg):
            pError("Config: mirror cfg file " + self.stdCfg + " does not exist.")
            sys.error(2)
        with open(self.stdCfg) as cfgFileHandle:
            self.mirrorCfg = json.load(cfgFileHandle)

    def openCustomCfg(self):
        # test that the file exists
        if  not os.path.isfile(self.optCfg):
            pError("Config: custom cfg file " + self.optCfg + " does not exist.")
            sys.error(2)
        # open
        with open(self.optCfg) as cfgFileHandle:
            mirrorCfg = json.load(cfgFileHandle)
        # update mirror's cfg
        self.mirrorCfg = update(self.mirrorCfg, mirrorCfg)

    def updatePlatform(self, platform):
        self.platform = platform
        self.printVerbose("get cfg for platform: " + self.platform)
        if self.platform in list(self.mirrorCfg.keys()):
            self.platformCfg = self.mirrorCfg[self.platform]
        else:
            pError("Config: platform " + self.platform + " not found in cfg file " + self.cfgFile)
            sys.error(2)

    def privateKey(self):
        key = "sshprivatekey"
        return self.getKeyValue(key)

    def remoteHost(self):
        key = "remotehost"
        return self.getKeyValue(key)

    def remoteUser(self):
        key = "remoteuser"
        return self.getKeyValue(key)

    def rootDir(self):
        key = "rootdir"
        return self.getKeyValue(key)

    def include(self):
        key = "include"
        return self.getKeyValue(key)

    def exclude(self):
        key = "exclude"
        return self.getKeyValue(key)

    def getKeyValue(self, key):
        if key in list(self.platformCfg.keys()):
            kvalue = self.platformCfg[key]
        else:
            pError("Config: key " + key + " not found")
            sys.error(2)
        return kvalue

    def getPlatform(self):
        return self.platform

def setRemoteUser(cfg):
    if cfg.remoteUser() != None:
        remoteuser = cfg.remoteUser()
    else:
        luser = getpass.getuser()
        if cfg.getPlatform() == "gcp":
            ruser = "ext_" + luser + "_uw_edu"
        else:
            ruser = luser
        remoteuser = ruser
    return remoteuser

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
    print( '\tRemote root directory: ' + rootdir)
    print( '\tPreserve root directory tree: ' + str(preserveTree))
    print( '\tSSH key: ' + sshprivatekey)
    print ('\tInclude pattern: ' + str(include))
    print ('\tExclude pattern: ' + str(exclude))
    print( '\tVersion: ' + version)
    print( '\tDebug: ' + str(debug))
    print( '\tExecute: ' + str(execute))
    print( '\tTest: ' + str(test))
    tmsg=time.asctime()
    print( '\tTime: ' + tmsg)
# begin time
tbegin=time.asctime()

# defaults
defPlatform = "gcp"
# parse input
parser = ArgumentParser( description = "Mirror copy (via rsync) preserving tree if same root dir; else copy specified path under root dir " )
parser.add_argument( "source", nargs = '+', help = "source of files to copy" )
parser.add_argument( "-c", "--cfgfile", help = "Custom cfg file name")
parser.add_argument( "-s", "--sshprivatekey",
                     help = "SSH private key [default: in cfg file]")
parser.add_argument( "-p", "--platform", default = defPlatform,
                     help = "Platform in cfg file [default: " + defPlatform + "]")
parser.add_argument( "-H", "--remotehost",
                     help = "Remote host [default: in cfg file]")
parser.add_argument( "-u", "--remoteuser",
                     help = "Remote user [default: based on platform")
parser.add_argument( "-R", "--rootdir",
                     help = "Root directory to mirror [default: in cfg file]")
parser.add_argument( "-i", "--include",
                     help = "Patterns (comma delimited) of file names to include (overrides -e) [default: none]" )
parser.add_argument( "-e", "--exclude",
                     help = "Patterns (comma delimited) of file names to exclude [default: none")
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
cfgfile = args.cfgfile
platform = args.platform
sshprivatekey = args.sshprivatekey
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
# create config object
cfg = Config(platform = platform, opt_cfg = cfgfile)

if sshprivatekey == None:
    sshprivatekey = cfg.privateKey()

if remotehost == None:
    remotehost = cfg.remoteHost()

if remoteuser == None:
    remoteuser = setRemoteUser(cfg)

if rootdir == None:
    rootdir = cfg.rootDir()

if include == None:
    include = cfg.include()

if exclude == None:
    exclude = cfg.exclude()
# get the rootdir of the source
slash = "/"
srcAbsPath = os.path.abspath(source)
srcRootdir = slash + srcAbsPath.split(slash)[1] + slash
# check remote rootdir (must have leading "/")
if rootdir[0] != slash:
    pError("Root directory " + rootdir + " is not valid - requires leading " + slash)
    sys.exit(2)
if rootdir[len(rootdir)-1] != slash:
    rootdir += slash
# process source - if rootdir and srcRoodir are the same, then we'll preserve the tree under rootdir
# but if the rootdirs are different, then we'll keep the specified src without convert to an absolute path
# which enables user to specify how much of the source tree to preserver (with '/./')
if srcRootdir == rootdir:
    preserveTree = True
    srcDir = os.path.dirname(srcAbsPath)
    srcBasename = os.path.basename(srcAbsPath)
    # remote stuff and direcory stuff
    # from the absolute source path (e.g., /projects/topmed/...) we need to enable folders
    # below root /projects by creating /projects/./ ...
    rsource = srcAbsPath.replace(rootdir,rootdir+"./")
else:
    rsource = source
    preserveTree = False
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

if summary:
    Summary("Upload Tree Summary")

# build thr rsync command
rscommand = "rsync -av -O --no-perms --update --chmod=Fg+w "
# ssh
rscommand += '"-e ssh -i ~/.ssh/' + sshprivatekey + '" '
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
