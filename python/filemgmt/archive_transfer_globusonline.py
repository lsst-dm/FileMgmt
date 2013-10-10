# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
"""

__version__ = "$Rev$"

import os
import copy

from coreutils.miscutils import *
from filemgmt.filemgmt_defs import *
import filemgmt.gonline as globonline

GO_USER = 'go_user'
X509_USER_PROXY = 'x509_user_proxy'
PROXY_VALID_HRS = 'proxy_valid_hrs'

class ArchiveTransferGlobusOnline():
    """
    """
    # assumes src and dst archives are not on same machine and should use GlobusOnline for transfers

    @staticmethod
    def requested_config_vals():
        return {GO_USER:'REQ', X509_USER_PROXY:'OPT', PROXY_VALID_HRS: 'OPT'}

    def __init__ (self, src_archive_info, dst_archive_info, archive_transfer_info, config=None):
        self.src_archive_info = src_archive_info 
        self.dst_archive_info = dst_archive_info 
        self.archive_transfer_info = archive_transfer_info
        self.config = config



    def blocking_transfer(self, filelist):
        print "blocking_transfer"
        print "\tfilelist: ", filelist

        srcroot = self.src_archive_info['root']
        dstroot = self.dst_archive_info['root']
        
        files2copy = {}
        problems = {}
        srctranslation = {}
        for srcfile, fileinfo in filelist.items():
            #if not re.match('^/', dstfile):   # if path is relative
            #    dstfile = "%s/%s" % (dstroot, dstfile)
            #elif not re.match('^%s/' % dstroot, dstfile): # check for archive root
            #    problems[srcfile] = {'dst': dstfile, 'err': "dstfile doesn't contain archive root"}
            #    continue

            #if not re.match('^/', srcfile):   # if path is relative
            #    origsrcfile = srcfile
            #    srcfile = "%s/%s" % (srcroot, srcfile)
            #    srctranslation[srcfile] = origsrcfile
            #elif not re.match('^%s/' % srcroot, srcfile): # check for archive root
            #    problems[srcfile] = {'dst': dstfile, 'err': "srcfile doesn't contain archive root"}
            #    continue
                
            files2copy[fileinfo['src']] = fileinfo['dst']


        credfile = None
        if X509_USER_PROXY in self.config:
            credfile = self.config[X509_USER_PROXY]
        elif 'X509_USER_PROXY' in os.environ:
            credfile = os.environ['X509_USER_PROXY']

        if credfile is None:
            fwdie('Error:  Cannot determine location of X509 proxy.  Either set in config or environment.', 1)


        proxy_valid_hrs = 12
        if PROXY_VALID_HRS in self.config:
            proxy_valid_hrs = self.config[PROXY_VALID_HRS]

        if GO_USER not in self.config:
            fwdie('Error:  Missing %s in config' % GO_USER, 1)

    
        goclient = globonline.DESGlobusOnline(self.src_archive_info, self.dst_archive_info, credfile, 
                                              self.config[GO_USER], proxy_valid_hrs)
        trans_results = goclient.blocking_transfer(files2copy)

        
        results = copy.deepcopy(filelist)
        for src, trans_info in trans_results.items():
            print "src = %s, trans_info = %s" % (src, trans_info)
            filename = parse_fullname(src, 2)
            if trans_info is not None:
                print "results[%s] = %s:" % (filename, results[filename])
                results[filename].update(trans_info)
            
        return results 
