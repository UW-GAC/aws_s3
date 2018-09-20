import json
import os
import sys

class awscontext(object):
    def __init__(self,ctx_file=None, ctx_version="2.0", verbose = False):
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
        self.sqs_ctxkey = "sqsctx"
        self.sqs_namekey = "name"
        self.sqs_urlkey = "url"
        # get the sqs ontexts
        key = self.sqs_ctxkey
        if key in ctxinfo:
            self.sqsctxs = ctxinfo[key]
            self.sqsctxnames = self.sqsctxs.keys()
        else:
            print("Error: " + key + " key not found in " + self.ctx_file)
            sys.exit(2)
        if self.verbose:
            for key,value in self.sqsctxs.iteritems():
                print( "\t>>>sqscontext: key: " + key + "  value: " + str(value))
        # get the contexts
        key = "context"
        if key in ctxinfo:
            self.contexts = ctxinfo[key]
            self.ctxnames = self.contexts.keys()
        else:
            print("Error: " + key + " key not found in " + self.ctx_file)
            sys.exit(2)
        if self.verbose:
            for key,value in self.contexts.iteritems():
                print( "\t>>>awscontext: key: " + key + "  value: " + str(value))
        # create list of profiles and bucket names
        self.profile_names = []
        self.bucket_names = []
        for ctxname,ctxdict in self.contexts.iteritems():
            if self.verbose:
                print( "\t>>>awscontext: key: " + ctxname + "  value: " + str(ctxdict))
            for key,value in ctxdict.iteritems():
                if key == "s3bucket":
                    self.bucket_names.append(value)
                elif key == "profile":
                    self.profile_names.append(value)

    def getctxnames(self):
        return self.ctxnames
    def getsqsctxnames(self):
        return self.sqsctxnames
    def getprofilenames(self):
        return self.profile_names
    def getbucketnames(self):
        return self.bucket_names
    def getctx(self, name_a):
        ctx = None
        if name_a in self.ctxnames:
            ctx = self.contexts[name_a]
        return ctx
    def getsqsctx(self, name_a):
        sqsctx = None
        if name_a in self.sqsctxnames:
            sqsctx = self.sqsctxnames[name_a]
        return sqsctx
    def getdefaultctx(self):
        key = 'default`'
        if key not in self.ctxnames:
            print("Error: " + key + " key not found in " + self.ctx_file)
            sys.exit(2)
        return self.contexts[key]
    def getsqsname(self, ctxname_a):
        name = None
        if ctxname_a in self.ctxnames:
            if self.sqs_ctxkey in self.contexts[ctxname_a]:
                sqsctxname = self.contexts[ctxname_a][self.sqs_ctxkey]
                if sqsctxname in self.sqsctxnames:
                    if self.sqs_namekey in self.sqsctxs[sqsctxname]:
                        name = self.sqsctxs[ctxname][self.sqs_namekey]
        return name
    def getsqsurl(self, ctxname_a):
        url = None
        if ctxname_a in self.ctxnames:
            if self.sqs_ctxkey in self.contexts[ctxname_a]:
                sqsctxname = self.contexts[ctxname_a][self.sqs_ctxkey]
                if sqsctxname in self.sqsctxnames:
                    if self.sqs_urlkey in self.sqsctxs[sqsctxname]:
                        url = self.sqsctxs[sqsctxname][self.sqs_urlkey]
        return url
    def getsqsurl_byname(self, sqsctxname_a):
        url = None
        if sqsctxname_a in self.sqsctxnames:
            if self.sqs_urlkey in self.sqsctxs[sqsctxname_a]:
                url = self.sqsctxs[sqsctxname_a][self.sqs_urlkey]
        return url
    def getbucketname(self, name_a):
        bn = None
        if name_a in self.ctxnames:
            bn = self.contexts[name_a]['s3bucket']
        return bn
    def getprofile(self, name_a):
        profile = None
        if name_a in self.ctxnames:
            profile = self.contexts[name_a]['profile']
        return profile
