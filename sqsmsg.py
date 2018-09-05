import      time
import      getpass

def encode(msg_a, timestamp=None, bywhom=None, typemsg='s3change'):
    if timestamp == None:
        timestamp = time.asctime()
    if bywhom == None:
        bywhom = getpass.getuser()
    mattr = {
                'type': typemsg,
                'msg': msg_a,
                'date': timestamp,
                'bywhom': bywhom
            }
    return str(mattr)

def decode(emsg_a):
    return eval(emsg_a)
