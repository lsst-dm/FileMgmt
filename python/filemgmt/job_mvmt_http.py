# $Id: job_mvmt_local.py 18938 2014-02-04 19:09:06Z mgower $
# $Rev:: 18938                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2014-02-04 13:09:06 #$:  # Date of last commit.

"""
"""

__version__ = "$Rev: 18938 $"

import os
import shutil
import copy

from coreutils.miscutils import *
from filemgmt.filemgmt_defs import *
import filemgmt.http_utils as http_utils

SUBMIT_DES_SERVICES = 'submit_des_services'
SUBMIT_DES_HTTP_SECTION = 'submit_des_http_section'

class JobArchiveHttp():
    """
    """
    # assumes home, target, and job dirs are read/write same machine

    @staticmethod
    def requested_config_vals():
        return {SUBMIT_DES_SERVICES:'REQ', SUBMIT_DES_HTTP_SECTION:'REQ'}

    def __init__ (self, homeinfo, targetinfo, mvmtinfo, config=None):
        self.home = homeinfo 
        self.target = targetinfo 
        self.mvmt = mvmtinfo
        self.config = config
        
        for x in (SUBMIT_DES_SERVICES, SUBMIT_DES_HTTP_SECTION):
            if x not in self.config:
                fwdie('Error:  Missing %s in config' % x, 1)
        self.HU = http_utils.HttpUtils(self.config[SUBMIT_DES_SERVICES],
                                       self.config[SUBMIT_DES_HTTP_SECTION])


    def home2job(self, filelist):
        # if staging outside job, this function shouldn't be called
        if self.home is None:
            raise Exception("Home archive info is None.   Should not be calling this function")
        
        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['src'] = self.home['root_http'] + '/' + finfo['src']

        return self.HU.copyfiles(absfilelist)


    def target2job(self, filelist):
        if self.target is None:
            raise Exception("Target archive info is None.   Should not be calling this function")
        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['src'] = self.target['root_http'] + '/' + finfo['src']
        return self.HU.copyfiles(absfilelist)


    def job2target(self, filelist):
        if self.target is None:
            raise Exception("Target archive info is None.   Should not be calling this function")
        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['dst'] = self.target['root_http'] + '/' + finfo['dst']
        results = self.HU.copyfiles(absfilelist)
        return results


    def job2home(self, filelist):
        # if staging outside job, this function shouldn't be called
        if self.home is None:
            raise Exception("Home archive info is None.   Should not be calling this function")
        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['dst'] = self.home['root_http'] + '/' + finfo['dst']
        results = self.HU.copyfiles(absfilelist)
        return results
