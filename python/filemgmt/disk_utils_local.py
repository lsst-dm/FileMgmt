# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
Generic routines for performing tasks on files that can be seen locally
"""

__version__ = "$Rev$"

import os
import shutil

from coreutils.miscutils import *
from filemgmt.filemgmt_defs import *


######################################################################
def get_file_disk_info(arg):
    """ Returns information about files on disk from given list or path"""

    if isinstance(arg) is list:
        return get_file_disk_info_list(arg)
    elif type(arg) is str:
        return get_file_disk_info_path(arg)
    else:
        fwdie("Error:  argument to get_file_disk_info isn't a list or a path (%s)" % type(arg), 1)
    

######################################################################
def get_file_disk_info_list(filelist):
    """ Returns information about files on disk from given list """

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



######################################################################
def get_file_disk_info_path(path):
    """ Returns information about files on disk from given path """
    # if relative path, is treated relative to current directory

    if not os.path.exists(path):
        fwdie("Error:  path does not exist (%s)" % (path), 1)

    fileinfo = {}
    for (dirpath, dirnames, filenames) in os.walk(path):
        for fname in filenames:
            d = {}
            (d['filename'], d['compression']) = parse_fullname(fname, 3)
            d['filesize'] = os.path.getsize("%s/%s" % (dirpath, fname))
            d['path'] = dirpath[len(root)+1:]
            if d['compression'] is None:
                compext = ""
            else:
                compext = d['compression']
            d['rel_filename'] = "%s/%s%s" % (d['path'], d['filename'], compext)
            fileinfo[fname] = d

    return fileinfo



######################################################################
def del_file_disk_list(filelist):
    """ Deletes files on disk from given list """
    # deletes any directories it made empty by deleting files

    #### TODO

    fileinfo = {}
    #paths = {}
    #for fname in filelist:
    #    if os.path.exists(fname):
    #        paths[os.path.dirname(fname)] = True
    #        fwdebug(0, "DISK_UTILS_LOCAL_DEBUG", "Deleting %s" % fname)
    #        #os.unlink(fname)
    #    else:
    #        fileinfo[fname] = { 'err': "Could not find file" }
    #
    # check if empty paths and remove
    # for p in paths.keys():
    #

    return fileinfo


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
                    coremakedirs(path)
                shutil.copy(src, dst)
        except Exception as err:
            filelist[filename]['err'] = str(err)
    return filelist
