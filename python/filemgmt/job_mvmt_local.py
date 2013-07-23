# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
"""

__version__ = "$Rev$"

import os
import shutil

from coreutils.miscutils import *
from filemgmt.filemgmt_defs import *
import mvmt_utils_local

class JobArchiveLocal():
    """
    """
    # assumes home, target, and job dirs are read/write same machine

    @staticmethod
    def requested_config_vals():
        return {}

    def __init__ (self, homeinfo, targetinfo, mvmtinfo, config=None):
        self.home = homeinfo 
        self.target = targetinfo 
        self.mvmt = mvmtinfo
        self.config = config


    def home2job(self, filelist):
        # if staging outside job, this function shouldn't be called
        if self.home is None:
            raise Exception("Home archive info is None.   Should not be calling this function")
        return mvmt_utils_local.copyfiles(filelist)


    def target2job(self, filelist):
        if self.target is None:
            raise Exception("Target archive info is None.   Should not be calling this function")
        return mvmt_utils_local.copyfiles(filelist)


    def job2target(self, filelist):
        if self.target is None:
            raise Exception("Target archive info is None.   Should not be calling this function")
        results = mvmt_utils_local.copyfiles(filelist)
        return results


    def job2home(self, filelist):
        # if staging outside job, this function shouldn't be called
        if self.home is None:
            raise Exception("Home archive info is None.   Should not be calling this function")
        results = mvmt_utils_local.copyfiles(filelist)
        return results
