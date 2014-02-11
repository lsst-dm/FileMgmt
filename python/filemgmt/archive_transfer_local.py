# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
"""

__version__ = "$Rev$"


import copy
from coreutils.miscutils import *
from filemgmt.filemgmt_defs import *
import filemgmt.disk_utils_local as disk_utils_local

class ArchiveTransferLocal():
    """
    """
    @staticmethod
    def requested_config_vals():
        return {}    # no extra values needed 

    # assumes home and target are on same machine

    def __init__ (self, src_archive_info, dst_archive_info, archive_transfer_info, config=None):
        self.src_archive_info = src_archive_info 
        self.dst_archive_info = dst_archive_info 
        self.archive_transfer_info = archive_transfer_info 
        self.config = config


    def blocking_transfer(self, filelist):
        fwdebug(0, "ARCHIVETRANSFER_DEBUG", "\tNumber files to transfer: %d" % len(filelist))
        fwdebug(1, "ARCHIVETRANSFER_DEBUG", "\tfilelist: %s" % filelist)

        srcroot = self.src_archive_info['root']
        dstroot = self.dst_archive_info['root']
        
        files2copy = copy.deepcopy(filelist)
        problems = {}
        srctranslation = {}
        for fname, finfo in files2copy.items():
            finfo['src'] = '%s/%s' % (srcroot, finfo['src'])
            finfo['dst'] = '%s/%s' % (dstroot, finfo['dst'])

        transresults = disk_utils_local.copyfiles(files2copy)

        return transresults 
