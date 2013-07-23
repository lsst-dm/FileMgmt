# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
"""

__version__ = "$Rev$"

from coreutils.miscutils import *
from filemgmt.filemgmt_defs import *

class BaseFileUtils ():
    """
        Base class for FileUtils to define needed functions
    """

    def __init__ (self, homeinfo, targetinfo, **kwargs):
        self.home = homeinfo
        self.target = targetinfo 
        self.kwargs = kwargs

    def archive2archive(self, filelist):
        print "base archive2archive"
        raise Exception("Missing archive2archive override")

    def home2target(self, filelist):
        print "base home2target"
        raise Exception("Missing home2target override")

    def home2job(self, filelist, homeinfo):
        print "base home2job"
        raise Exception("Missing home2job override")
        return self._copyfiles(self, filelist)

    def target2job(self, filelist, targetinfo):
        print "base target2job"
        raise Exception("Missing target2job override")

    def job2target(self, filelist, targetinfo):
        print "base job2target"
        raise Exception("Missing job2target override")
        return self._copyfiles(self, filelist)

    def job2home(self, filelist, homeinfo):
        print "base job2home"
        raise Exception("Missing job2home override")

    def target2home(self, filelist, homeinfo, targetinfo):
        print "base target2home"
        raise Exception("Missing target2home override")
