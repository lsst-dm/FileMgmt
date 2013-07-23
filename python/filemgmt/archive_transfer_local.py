# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
"""

__version__ = "$Rev$"


from coreutils.miscutils import *
from filemgmt.filemgmt_defs import *
import filemgmt.mvmt_utils_local as mvmt_utils_local

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
        print "blocking_transfer"
        print "\tfilelist: ", filelist

        srcroot = self.src_archive_info['root']
        dstroot = self.dst_archive_info['root']
        
        files2copy = {}
        problems = {}
        srctranslation = {}
        for srcfile, dstfile in filelist.items():
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
                
            files2copy[srcfile] = dstfile

        problems = mvmt_utils_local.copyfiles(files2copy)

        return problems 
