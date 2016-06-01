# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
Generic routines for performing tasks on files that can be seen locally
"""

__version__ = "$Rev$"

import os
import sys
import shutil
import hashlib
import errno
import time

import despymisc.miscutils as miscutils


######################################################################
def get_md5sum_file(fullname, blksize = 2**15):
    """ Returns md5 checksum for given file """

    md5 = hashlib.md5()
    with open(fullname, 'rb') as f:
        for chunk in iter(lambda: f.read(blksize), ''):
            md5.update(chunk)
    return md5.hexdigest()

######################################################################
def get_file_disk_info(arg):
    """ Returns information about files on disk from given list or path"""

    if isinstance(arg) is list:
        return get_file_disk_info_list(arg)
    elif type(arg) is str:
        return get_file_disk_info_path(arg)
    else:
        miscutils.fwdie("Error:  argument to get_file_disk_info isn't a list or a path (%s)" % type(arg), 1)
    
######################################################################
def get_single_file_disk_info(fname, save_md5sum=False, archive_root=None):
    if miscutils.fwdebug_check(3, "DISK_UTILS_LOCAL_DEBUG"):
        miscutils.fwdebug_print("fname=%s, save_md5sum=%s, archive_root=%s" % \
                                (fname, save_md5sum, archive_root))

    parsemask = miscutils.CU_PARSE_PATH | miscutils.CU_PARSE_FILENAME | miscutils.CU_PARSE_COMPRESSION

    (path, filename, compress) = miscutils.parse_fullname(fname, parsemask)
    if miscutils.fwdebug_check(3, "DISK_UTILS_LOCAL_DEBUG"):
        miscutils.fwdebug_print("path=%s, filename=%s, compress=%s" % (path,filename,compress))

    fdict = {
        'filename' : filename,
        'compression': compress,
        'path': path,
        'filesize': os.path.getsize(fname)
        }

    if save_md5sum:
        fdict['md5sum'] = get_md5sum_file(fname)

    if archive_root and path.startswith('/'):
        fdict['relpath'] = path[len(archive_root)+1:]

        if compress is None:  
            compext = ""
        else:
            compext = compress

        fdict['rel_filename'] = "%s/%s%s" % (fdict['relpath'], filename, compext)

    return fdict


######################################################################
def get_file_disk_info_list(filelist, save_md5sum=False):
    """ Returns information about files on disk from given list """

    fileinfo = {}
    for fname in filelist:
        if os.path.exists(fname):
            fileinfo[fname] = get_single_file_disk_info(fname, save_md5sum)
        else:
            fileinfo[fname] = { 'err': "Could not find file" }

    return fileinfo



######################################################################
def get_file_disk_info_path(path, save_md5sum=False):
    """ Returns information about files on disk from given path """
    # if relative path, is treated relative to current directory

    if not os.path.exists(path):
        miscutils.fwdie("Error:  path does not exist (%s)" % (path), 1)

    fileinfo = {}
    for (dirpath, dirnames, filenames) in os.walk(path):
        for name in filenames:
            fname = os.path.join(dirpath, name)
            fileinfo[fname] = get_single_file_disk_info(fname, save_md5sum)

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
    #        if miscutils.fwdebug_check(3, "DISK_UTILS_LOCAL_DEBUG"):
    #           miscutils.fwdebug_print("Deleting %s" % fname)
    #        #os.unlink(fname)
    #    else:
    #        fileinfo[fname] = { 'err': "Could not find file" }
    #
    # check if empty paths and remove
    # for p in paths.keys():
    #

    return fileinfo


######################################################################
def copyfiles(filelist, tstats):
    """ Copies files in given src,dst in filelist """

    results = {}
    status = 0
    for filename, fdict in filelist.items():
        fsize = 0
        try:
            src = fdict['src']
            dst = fdict['dst']

            if 'filesize' in fdict:
                fsize = fdict['filesize']
            elif os.path.exists(src):
                fsize = os.path.getsize(src)

            if not os.path.exists(dst):
                if tstats is not None:
                    tstats.stat_beg_file(filename)
                path = os.path.dirname(dst)
                if len(path) > 0 and not os.path.exists(path):
                    miscutils.coremakedirs(path)
                shutil.copy(src, dst)
                if tstats is not None:
                    tstats.stat_end_file(0, fsize)
        except Exception:
            status = 1
            if tstats is not None:
                tstats.stat_end_file(1, fsize)
            (etype, value, trback) = sys.exc_info()
            filelist[filename]['err'] = str(value)
    return (status, filelist)

######################################################################
def remove_file_if_exists(filename):
    try:
        os.remove(filename)
    except OSError as e:
        if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            raise
# end remove_file_if_exists

######################################################################
def get_files_from_disk(relpath, archive_root, check_md5sum=False, debug=False):
    """ Check disk to get list of files within that path inside the archive 

        Parameters
        ----------
        archive_root : str
            The base root of the relpath entry

        check_md5sum : bool
            Whether or not to compare md5sums

        debug : bool
            Whether or not to report debugging info

        Returns
        -------
        A dictionary contianing the info about the files on disk (filesize, md5sum, compression, filename, path)

    """

    start_time = time.time()
    if debug:
        print "Getting file information from disk: BEG"

    files_from_disk = {}
    for (dirpath, dirnames, filenames) in os.walk(os.path.join(archive_root,relpath)):
        for filename in filenames:
            fullname = '%s/%s' % (dirpath, filename)
            files_from_disk[filename] = get_single_file_disk_info(fullname, check_md5sum, archive_root)

    end_time = time.time()
    if debug:
        print "Getting file information from disk: END (%s secs)" % (end_time - start_time)
    return files_from_disk

####################################################################
def compare_db_disk(files_from_db, files_from_disk, check_md5sum, check_filesize, debug=False,archive_root=""):
    """ Compare file info from DB to info from disk 

        Parameters
        ----------
        file_from_db : dict
            Dicitonary containing the file info from the database

        files_from_disk : dict
            Dictionary containing the file info from disk

        check_md5sum : bool
            Whether or not to report the md5sum comparision

        check_filesize : bool
            Whether or not to report the filesize comparison

        debug : bool
            Whenther or not to report debugging info
            Default: False

        archive_root : str
            The archive root path
            Default : False

        Returns
        -------
        None
    """

    start_time = time.time()
    if debug:
        print "Comparing file information: BEG"
    comparison_info = { 
                        'equal': [],
                        'dbonly': [],
                        'diskonly': [],
                        'path': [],
                        'filesize': []
                      }
    if check_md5sum:
        comparison_info['md5sum'] = []

    if check_filesize:
        comparison_info['filesize'] = []

    allfiles = Set(files_from_db).union(Set(files_from_disk))
    for fname in allfiles:
        if fname in files_from_db:
            if fname in files_from_disk:
                fdisk = files_from_disk[fname]
                fdb = files_from_db[fname]
                if fdisk['relpath'] == fdb['path']:
                    if fdisk['filesize'] == fdb['filesize']:
                        if check_md5sum:
                            if fdisk['md5sum'] == fdb['md5sum']:
                                comparison_info['equal'].append(fname)
                            else:
                                comparison_info['md5sum'].append(fname)
                        else:
                            comparison_info['equal'].append(fname)
                    else:
                        comparison_info['filesize'].append(fname)
                else:
                    comparison_info['path'].append(fname)
            else:
                comparison_info['dbonly'].append(fname)
        else:
            comparison_info['diskonly'].append(fname)

    end_time = time.time()
    if debug:
        print "Comparing file information: END (%s secs)" % (end_time - start_time)
    return comparison_info
