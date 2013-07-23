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
def copyfiles(filelist):
    """ Copies files in given src,dst in filelist """
    results = {}
    for filename, fdict in filelist.items():
        try:
            src = fdict['src']
            dst = fdict['dst']
            if not os.path.exists(dst):
                path = os.path.dirname(dst)
                if len(path) > 0 and not os.path.exists(path):
                    os.makedirs(path)
                shutil.copy(src, dst)
        except Exception as err:
            filelist[filename]['err'] = str(err)
    return filelist
