#!/usr/bin/env python

# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

import copy
import time
from coreutils.miscutils import *
from filemgmt.filemgmt_defs import *

def get_config_vals(archive_info, config, keylist):
    """ Search given dicts for specific values """
    info = {}
    for k, stat in keylist.items():
        if archive_info is not None and k in archive_info:
            info[k] = archive_info[k]
        elif config is not None and k in config:
            info[k] = config[k]
        elif stat.lower() == 'req':
            fwdebug(0, ARCHIVE_COPY_DEBUG, '******************************')
            fwdebug(0, ARCHIVE_COPY_DEBUG, 'keylist = %s' % keylist)
            fwdebug(0, ARCHIVE_COPY_DEBUG, 'archive_info = %s' % archive_info)
            fwdebug(0, ARCHIVE_COPY_DEBUG, 'config = %s' % config)
            fwdie('Error: Could not find required key (%s)' % k, 1, 2)
    return info
              


def archive_copy(src_archive_info, dst_archive_info, archive_transfer_info, filelist, config=None):
    src_archive = src_archive_info['name']
    dst_archive = dst_archive_info['name']

    ## check which files are already on dst
    # dynamically load filemgmt class for dst
    dstfilemgmt_class = dynamically_load_class(dst_archive_info['filemgmt'])

    valDict = get_config_vals(dst_archive_info, config, dstfilemgmt_class.requested_config_vals())
    dstfilemgmt = dstfilemgmt_class(config=valDict)

    fwdebug(0, "ARCHIVE_COPY_DEBUG", "dst_archive = %s" % dst_archive)
    dst_file_archive_info = dstfilemgmt.get_file_archive_info(filelist, dst_archive, FM_PREFER_UNCOMPRESSED)
    fwdebug(0, "ARCHIVE_COPY_DEBUG", "dst_file_archive_info %s" % dst_file_archive_info)
    files2stage = set(filelist) - set(dst_file_archive_info.keys())

    fwdebug(0, "ARCHIVE_COPY_DEBUG", "files to stage %s" % files2stage)

    if files2stage is not None and len(files2stage) > 0:
        ## Stage files not already on dst
        # dynamically load filemgmt class for src
        srcfilemgmt_class = dynamically_load_class(src_archive_info['filemgmt'])
        valDict = get_config_vals(src_archive_info, config, srcfilemgmt_class.requested_config_vals())
        srcfilemgmt = srcfilemgmt_class(config=valDict)

        # get archive paths for files in home archive
        src_file_archive_info = srcfilemgmt.get_file_archive_info(files2stage, src_archive, FM_PREFER_COMPRESSED)
        missing_files = set(files2stage) - set(src_file_archive_info.keys())
        fwdebug(0, "ARCHIVE_COPY_DEBUG", "files not on src archive %s" % missing_files)

        if missing_files is not None and len(missing_files) > 0:
            print "Error:  Could not find the following files on the src archive"
            for f in missing_files:
                print "\t%s" % f
            fwdie("Error: Missing files", 1)

        # dst rel path will be same as src rel path
        # create full paths for home archive and target archive
        src_root = src_archive_info['root']
        dst_root = dst_archive_info['root']
        files2copy = {}
        for filename, fileinfo in src_file_archive_info.items():
            fwdebug(0, "ARCHIVE_COPY_DEBUG", "%s: fileinfo = %s" % (filename, fileinfo))
            files2copy[filename] = copy.deepcopy(fileinfo)
            files2copy[filename]['src'] = "%s/%s" % (src_root, fileinfo['rel_filename'])
            files2copy[filename]['dst'] = "%s/%s" % (dst_root, fileinfo['rel_filename'])

        ## call blocking transfer function
        # import archive 2 archive class
        transobj = None
        try:
            transfer_class = dynamically_load_class(archive_transfer_info[src_archive][dst_archive]['transfer'])
            valDict = get_config_vals(archive_transfer_info, config, transfer_class.requested_config_vals())
            transobj = transfer_class(src_archive_info, dst_archive_info, archive_transfer_info[src_archive][dst_archive], valDict)
        except Exception as err:
            print "ERROR\nError: creating archive transfer object\n%s" % err
            raise

        starttime = time.time()    # save start time
        results = transobj.blocking_transfer(files2copy)
        endtime = time.time()     # save end time
        print "\tTransfering %s file(s) took %s seconds" % (len(files2copy),endtime - starttime)

        files2register = {}
        problemfiles = {}
        for f, finfo in results.items():
            if 'err' in finfo:
                problemfiles[f] = finfo
            else:
                files2register[f] = finfo

        if problemfiles is not None and len(problemfiles) > 0:
            print "Error: Problems copying files from home archive to target archive"
            for f in problems:
                print "\t%s %s: %s", f, problemfiles['dst'], problemfiles['err']
        elif len(files2register) > 0:
            regprobs = dstfilemgmt.register_file_in_archive(files2register, {'archive': dst_archive})
            if regprobs is not None and len(regprobs) > 0:
                problemfiles.update(regprobs)

            if len(problemfiles) > 0:
                print "ERROR\n\n\nError: putting %0d files into archive" % len(problemfiles)
                for file in problemfiles:
                    print file, problemfiles[file]
                    sys.exit(1)
            else:
                dstfilemgmt.commit()


        # if db, save staging info to DB
        # todo

    fwdebug(0, "ARCHIVE_COPY_DEBUG", "END\n\n")
