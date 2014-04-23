# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
"""

__version__ = "$Rev$"

import os
import shutil
import copy

from coreutils.miscutils import *
from filemgmt.filemgmt_defs import *
import filemgmt.disk_utils_local as disk_utils_local

class JobArchiveLocal():
    """
    """
    # assumes home, target, and job dirs are read/write same machine

    @staticmethod
    def requested_config_vals():
        return {}

    def __init__ (self, homeinfo, targetinfo, mvmtinfo, tlogger, tstats, config=None):
        self.home = homeinfo 
        self.target = targetinfo 
        self.mvmt = mvmtinfo
        self.config = config
        self.tlogger = tlogger
        self.tstats = tstats


    def home2job(self, filelist):
        fwdebug(0, "JOBFILEMVMT_DEBUG", "len(filelist)=%s" % len(filelist))
        fwdebug(4, "JOBFILEMVMT_DEBUG", "filelist=%s" % filelist)

        # if staging outside job, this function shouldn't be called
        if self.home is None:
            if tlogger is not None:
                tlogger.critical("Home archive info is None.   Should not be calling this function")
            raise Exception("Home archive info is None.   Should not be calling this function")
        
        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['src'] = self.home['root'] + '/' + finfo['src']

        return disk_utils_local.copyfiles(absfilelist, self.tlogger, self.tstats)


    def target2job(self, filelist):
        fwdebug(0, "JOBFILEMVMT_DEBUG", "len(filelist)=%s" % len(filelist))
        fwdebug(4, "JOBFILEMVMT_DEBUG", "filelist=%s" % filelist)
        if self.target is None:
            if tlogger is not None:
                tlogger.critical("Target archive info is None.   Should not be calling this function")
            raise Exception("Target archive info is None.   Should not be calling this function")
        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['src'] = self.target['root'] + '/' + finfo['src']
        return disk_utils_local.copyfiles(absfilelist, self.tlogger, self.tstats)


    def job2target(self, filelist):
        fwdebug(0, "JOBFILEMVMT_DEBUG", "len(filelist)=%s" % len(filelist))
        fwdebug(4, "JOBFILEMVMT_DEBUG", "filelist=%s" % filelist)
        if self.target is None:
            if tlogger is not None:
                tlogger.critical("Target archive info is None.   Should not be calling this function")
            raise Exception("Target archive info is None.   Should not be calling this function")
        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['dst'] = self.target['root'] + '/' + finfo['dst']
        results = disk_utils_local.copyfiles(absfilelist, self.tlogger, self.tstats)
        return results


    def job2home(self, filelist):
        fwdebug(0, "JOBFILEMVMT_DEBUG", "len(filelist)=%s" % len(filelist))
        fwdebug(4, "JOBFILEMVMT_DEBUG", "filelist=%s" % filelist)
        # if staging outside job, this function shouldn't be called
        if self.home is None:
            if tlogger is not None:
                tlogger.critical("Home archive info is None.   Should not be calling this function")
            raise Exception("Home archive info is None.   Should not be calling this function")
        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['dst'] = self.home['root'] + '/' + finfo['dst']
        results = disk_utils_local.copyfiles(absfilelist, self.tlogger, self.tstats)
        return results
