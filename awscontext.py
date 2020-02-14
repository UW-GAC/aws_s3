import json
import os
import sys

class awscontext(object):
    def __init__(self,ctx_file=None, ctx_version="3.0", verbose = False):
        self.verbose = verbose
        self.ctx_file = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])),
                                     "awscontext.json")
        if self.verbose:
            print(">>>awscontext: ctx file is: " + self.ctx_file)
        if ctx_file != None:
            self.ctx_file = ctx_file
        # open the json ctx file
        with open(self.ctx_file) as cfh:
            ctxinfo = json.load(cfh)
        # check version
        key = "version"
        if key in ctxinfo:
            if ctxinfo[key] != ctx_version:
                print("Error: version of : " + self.ctx_file + " should be " + ctx_version +
                      " not " + ctxinfo[key])
                sys.exit(2)
        else:
            print("Error: " + key + " key not found in " + self.ctx_file)
            sys.exit(2)
        self.key_context = "context"
        self.key_profile = "profile"
        self.key_bucket = "s3bucket"
        # get the the context dict
        key = self.key_context
        if key in ctxinfo:
            self.cdict = ctxinfo[key]
            self.cdict_keys = self.cdict.keys()
        else:
            print("Error: " + key + " key not found in " + self.ctx_file)
            sys.exit(2)
        if self.verbose:
            for key,value in self.cdict.iteritems():
                print( "\t>>>Context dictionary: key: " + key + "  value: " + str(value))
    def getctxnames(self):
        return self.cdict_keys
    def getprofile(self, ctxname_a):
        profile = None
        if ctxname_a in self.cdict_keys:
            profile = self.cdict[ctxname_a][self.key_profile]
        return profile
    def getbucketname(self, ctxname_a):
        bn = None
        if ctxname_a in self.cdict_keys:
            bn = self.cdict[ctxname_a][self.key_bucket]
        return bn
    def getctx(self, ctxname_a):
        ctx = None
        if ctxname_a in self.cdict_keys:
            ctx = self.cdict[ctxname_a]
        return ctx
