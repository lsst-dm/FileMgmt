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


######################################################################
def get_file_disk_info(filelist):
    """ Returns information about file on disk from given list """
    fileinfo = {}
    for fname in filelist:
        if os.path.exists(fname):
            (path, filename, compress) = parse_fullname(fname, 7)
            fileinfo[fname] = {
                'filename' : filename,
                'compression': compress,
                'path': path,
                'filesize': os.path.getsize(fname)}
        else:
            fileinfo[fname] = { 'err': "Could not find file" }

    return fileinfo
