#!/usr/bin/env python

import copy
import sys
import time
import despymisc.miscutils as miscutils
import filemgmt.filemgmt_defs as fmdefs


def get_config_vals(archive_info, config, keylist):
    """ Search given dicts for specific values """
    info = {}
    for k, stat in list(keylist.items()):
        if archive_info is not None and k in archive_info:
            info[k] = archive_info[k]
        elif config is not None and k in config:
            info[k] = config[k]
        elif stat.lower() == 'req':
            miscutils.fwdebug_print('******************************')
            miscutils.fwdebug_print('keylist = %s' % keylist)
            miscutils.fwdebug_print('archive_info = %s' % archive_info)
            miscutils.fwdebug_print('config = %s' % config)
            miscutils.fwdie('Error: Could not find required key (%s)' % k, 1, 2)
    return info


def archive_copy(src_archive_info, dst_archive_info, archive_transfer_info, filelist, config=None):
    if miscutils.fwdebug_check(3, "ARCHIVE_TRANSFER_UTILS_DEBUG"):
        miscutils.fwdebug_print("BEG")

    src_archive = src_archive_info['name']
    dst_archive = dst_archive_info['name']

    ## check which files are already on dst
    # dynamically load filemgmt class for dst
    dstfilemgmt_class = miscutils.dynamically_load_class(dst_archive_info['filemgmt'])

    valDict = get_config_vals(dst_archive_info, config, dstfilemgmt_class.requested_config_vals())
    dstfilemgmt = dstfilemgmt_class(config=valDict)

    if miscutils.fwdebug_check(0, "ARCHIVE_TRANSFER_UTILS_DEBUG"):
        miscutils.fwdebug_print("dst_archive = %s" % dst_archive)
    dst_file_archive_info = dstfilemgmt.get_file_archive_info(filelist, dst_archive,
                                                              fmdefs.FM_PREFER_UNCOMPRESSED)
    if miscutils.fwdebug_check(3, "ARCHIVE_TRANSFER_UTILS_DEBUG"):
        miscutils.fwdebug_print("number of files already at dst %s" % len(dst_file_archive_info))
    if miscutils.fwdebug_check(6, "ARCHIVE_TRANSFER_UTILS_DEBUG"):
        miscutils.fwdebug_print("dst_file_archive_info %s" % dst_file_archive_info)
    files2stage = set(filelist) - set(dst_file_archive_info.keys())

    if miscutils.fwdebug_check(3, "ARCHIVE_TRANSFER_UTILS_DEBUG"):
        miscutils.fwdebug_print("number of files to stage %s" % len(files2stage))
    if miscutils.fwdebug_check(6, "ARCHIVE_TRANSFER_UTILS_DEBUG"):
        miscutils.fwdebug_print("files to stage %s" % files2stage)

    if files2stage is not None and len(files2stage) > 0:
        ## Stage files not already on dst
        # dynamically load filemgmt class for src
        srcfilemgmt_class = miscutils.dynamically_load_class(src_archive_info['filemgmt'])
        valDict = get_config_vals(src_archive_info, config, srcfilemgmt_class.requested_config_vals())
        srcfilemgmt = srcfilemgmt_class(config=valDict)

        # get archive paths for files in home archive
        src_file_archive_info = srcfilemgmt.get_file_archive_info(
            files2stage, src_archive, fmdefs.FM_PREFER_COMPRESSED)
        missing_files = set(files2stage) - set(src_file_archive_info.keys())

        if missing_files is not None and len(missing_files) > 0:
            print("Error:  Could not find the following files on the src archive")
            for f in missing_files:
                print("\t%s" % f)
            miscutils.fwdie("Error: Missing files", 1)

        # dst rel path will be same as src rel path
        # create full paths for home archive and target archive
        #src_root = src_archive_info['root']
        #dst_root = dst_archive_info['root']
        files2copy = {}
        for filename, fileinfo in list(src_file_archive_info.items()):
            if miscutils.fwdebug_check(6, "ARCHIVE_TRANSFER_UTILS_DEBUG"):
                miscutils.fwdebug_print("%s: fileinfo = %s" % (filename, fileinfo))
            files2copy[filename] = copy.deepcopy(fileinfo)
            #files2copy[filename]['src'] = "%s/%s" % (src_root, fileinfo['rel_filename'])
            #iles2copy[filename]['dst'] = "%s/%s" % (dst_root, fileinfo['rel_filename'])
            files2copy[filename]['src'] = fileinfo['rel_filename']
            files2copy[filename]['dst'] = fileinfo['rel_filename']

        ## call blocking transfer function
        transinfo = None
        if src_archive in archive_transfer_info and dst_archive in archive_transfer_info[src_archive]:
            transinfo = archive_transfer_info[src_archive][dst_archive]
        elif dst_archive in archive_transfer_info and src_archive in archive_transfer_info[dst_archive]:
            transinfo = archive_transfer_info[dst_archive][src_archive]
        else:
            miscutils.fwdie("Error:  Could not determine transfer class for %s and %s" %
                            (src_archive, dst_archive), 1)

        # import archive 2 archive class
        print("loading archive transfer class: %s" % transinfo['transfer'])
        transobj = None
        try:
            transfer_class = miscutils.dynamically_load_class(transinfo['transfer'])
            valDict = get_config_vals(transinfo, config, transfer_class.requested_config_vals())
            transobj = transfer_class(src_archive_info, dst_archive_info, transinfo, valDict)
        except Exception as err:
            print("ERROR\nError: creating archive transfer object\n%s" % err)
            raise

        starttime = time.time()    # save start time
        results = transobj.blocking_transfer(files2copy)
        endtime = time.time()     # save end time
        print("\tTransfering %s file(s) took %s seconds" % (len(files2copy), endtime - starttime))

        files2register = {}
        problemfiles = {}
        for f, finfo in list(results.items()):
            if 'err' in finfo:
                problemfiles[f] = finfo
            else:
                files2register[f] = finfo

        if problemfiles is not None and len(problemfiles) > 0:
            print("Error: Problems copying files from home archive to target archive")
            print(problemfiles)
            for f, pinfo in list(problemfiles.items()):
                print("\t%s %s -> %s: %s" % (f, pinfo['src'], pinfo['dst'], pinfo['err']))
        elif len(files2register) > 0:
            regprobs = dstfilemgmt.register_file_in_archive(files2register, dst_archive)
            if regprobs is not None and len(regprobs) > 0:
                problemfiles.update(regprobs)

            if len(problemfiles) > 0:
                print("ERROR\n\n\nError: putting %0d files into archive" % len(problemfiles))
                for file in problemfiles:
                    print(file, problemfiles[file])
                    sys.exit(1)
            else:
                dstfilemgmt.commit()

        # if db, save staging info to DB
        # todo

    if miscutils.fwdebug_check(3, "ARCHIVE_TRANSFER_UTILS_DEBUG"):
        miscutils.fwdebug_print("END\n\n")


def archive_copy_dir(src_archive_info, dst_archive_info, archive_transfer_info, relpath, config=None):
    # relpath is relative path within archive
    # this function doesn't check which files already exist on dst.

    if miscutils.fwdebug_check(3, "ARCHIVE_TRANSFER_UTILS_DEBUG"):
        miscutils.fwdebug_print("BEG")

    src_archive = src_archive_info['name']
    dst_archive = dst_archive_info['name']

    ## check which files are already on dst
    # dynamically load filemgmt class for dst
    dstfilemgmt_class = miscutils.dynamically_load_class(dst_archive_info['filemgmt'])

    valDict = get_config_vals(dst_archive_info, config, dstfilemgmt_class.requested_config_vals())
    dstfilemgmt = dstfilemgmt_class(config=valDict)

    if miscutils.fwdebug_check(3, "ARCHIVE_TRANSFER_UTILS_DEBUG"):
        miscutils.fwdebug_print("dst_archive = %s" % dst_archive)

    ## dynamically load filemgmt class for src
    #srcfilemgmt_class = miscutils.dynamically_load_class(src_archive_info['filemgmt'])
    #valDict = get_config_vals(src_archive_info, config, srcfilemgmt_class.requested_config_vals())
    #srcfilemgmt = srcfilemgmt_class(config=valDict)
    #
    ## get archive paths for files in home archive
    #src_file_archive_info = srcfilemgmt.get_file_archive_info(files2stage, src_archive, fwdefs.FM_PREFER_COMPRESSED)
    #missing_files = set(files2stage) - set(src_file_archive_info.keys())
    #fwdebug(0, "ARCHIVE_TRANSFER_UTILS_DEBUG", "files not on src archive %s" % missing_files)

    #    if missing_files is not None and len(missing_files) > 0:
    #        print "Error:  Could not find the following files on the src archive"
    #        for f in missing_files:
    #            print "\t%s" % f
    #        miscutils.fwdie("Error: Missing files", 1)

    # dst rel path will be same as src rel path
    transinfo = None
    if src_archive in archive_transfer_info and dst_archive in archive_transfer_info[src_archive]:
        transinfo = archive_transfer_info[src_archive][dst_archive]
    elif dst_archive in archive_transfer_info and src_archive in archive_transfer_info[dst_archive]:
        transinfo = archive_transfer_info[dst_archive][src_archive]
    else:
        miscutils.fwdie("Error:  Could not determine transfer class for %s and %s" %
                        (src_archive, dst_archive), 1)

    # import archive 2 archive class
    transobj = None
    try:
        transfer_class = miscutils.dynamically_load_class(transinfo['transfer'])
        valDict = get_config_vals(transinfo, config, transfer_class.requested_config_vals())
        transobj = transfer_class(src_archive_info, dst_archive_info, transinfo, valDict)
    except Exception as err:
        print("ERROR\nError: creating archive transfer object\n%s" % err)
        raise

    starttime = time.time()    # save start time
    transresults = transobj.transfer_directory(relpath)
    endtime = time.time()     # save end time
    print("\tTransfering directory %s took %s seconds" % (relpath, endtime - starttime))

    files2register = {}
    problemfiles = {}
    for f, finfo in list(transresults.items()):
        if 'err' in finfo:
            problemfiles[f] = finfo
        else:
            files2register[f] = finfo

    if problemfiles is not None and len(problemfiles) > 0:
        print("Error: Problems copying files from home archive to target archive")
        for f in problemfiles:
            print(problemfiles[f])
            print("\t%s %s: %s" % (f, problemfiles[f]['dst'], problemfiles[f]['err']))
    elif len(files2register) > 0:
        regprobs = dstfilemgmt.register_file_in_archive(files2register, dst_archive)
        if regprobs is not None and len(regprobs) > 0:
            problemfiles.update(regprobs)

        if len(problemfiles) > 0:
            print("ERROR\n\n\nError: putting %0d files into archive" % len(problemfiles))
            for file in problemfiles:
                print(file, problemfiles[file])
                sys.exit(1)
        else:
            dstfilemgmt.commit()

        # if db, save staging info to DB
        # todo

    if miscutils.fwdebug_check(3, "ARCHIVE_TRANSFER_UTILS_DEBUG"):
        miscutils.fwdebug_print("END\n\n")
