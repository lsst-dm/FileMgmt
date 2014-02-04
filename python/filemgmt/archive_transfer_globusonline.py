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
        #print "blocking_transfer"
        #print "\tfilelist: ", filelist

        srcroot = self.src_archive_info['root']
        dstroot = self.dst_archive_info['root']
        
        files2copy = copy.deepcopy(filelist)
        problems = {}
        srctranslation = {}
        for fname, fileinfo in filelist.items():
            files2copy[fname]['src'] = "%s/%s" % (srcroot, files2copy[fname]['src'])
            files2copy[fname]['dst'] = "%s/%s" % (dstroot, files2copy[fname]['dst'])


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

        #retresults = copy.deepcopy(filelist)
        #for src, trans_info in trans_results.items():
        #    print "\n\n\nsrc = %s, trans_info = %s" % (src, trans_info)
        #    filename = parse_fullname(src, CU_PARSE_FILENAME)
        #    if trans_info is not None:
        #        retresults[filename].update(trans_info)
            
        return trans_results 
