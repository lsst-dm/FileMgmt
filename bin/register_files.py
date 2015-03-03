#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

""" Program to ingest data files that were created external to framework """

import tempfile
import argparse
import subprocess
import os
import re
import sys
import time
from collections import OrderedDict

import wrappers.WrapperUtils as wraputils    # read metadata from fits file
import filemgmt.disk_utils_local as diskutils
import filemgmt.filemgmt_defs as fmdefs
import despymisc.miscutils as miscutils
import intgutils.metautils as metautils
import intgutils.metadefs as imetadefs

VERSION = '$Rev$'


###########################################################################
def save_register_info(filemgmt, task_id, provmsg, commit):
    row = {'task_id': task_id, 'prov_msg': provmsg}
    filemgmt.basic_insert_row('FILE_REGISTRATION', row)
    if commit:
        filemgmt.commit()

###########################################################################
def parse_provided_list(listname):
    """ create dictionary of files from list in file """

    #cwd = os.getcwd()
    cwd = os.getenv('PWD')  # don't use getcwd as it canonicallizes path
                            # which is not what we want for links internal to archive

    filelist = {}
    try:
        with open(listname, "r") as fh:
            for line in fh:
                (fullname, filetype) = miscutils.fwsplit(line, ',')
                if fullname[0] != '/':
                    fullname = cwd + '/' + fullname

                if not os.path.exists(fullname):
                    miscutils.fwdie("Error:   could not find file to register:  %s" % fullname, 1)

                (path, fname) = os.path.split(fullname)
                if fname in filelist:
                    print "Error:   Found duplicate filenames in list:  %s"
                    print "\t%s" % fullname
                    print "\t%s" % filelist[fname]['fullname']
                    miscutils.fwdie("Error:   Found duplicate filenames in list:  %s" % fname, 1)
                filelist[fname] = {'path': path, 'filetype': filetype, 'fullname':fullname, 'fname':fname}
    except Exception as err:
        miscutils.fwdie("Error: Problems reading file '%s': %s" % (listname, err), 1)

    return filelist


###########################################################################
def get_list_filenames(ingestpath, filetype):
    """ create a dictionary of files in given path """

    if ingestpath[0] != '/':
        cwd = os.getenv('PWD')  # don't use getcwd as it canonicallizes path
                                # which is not what we want for links internal to archive
        ingestpath = cwd + '/' + ingestpath

    if not os.path.exists(ingestpath):
        miscutils.fwdie("Error:   could not find ingestpath:  %s" % ingestpath, 1)

    filelist = {}
    for (dirpath, dirnames, filenames) in os.walk(ingestpath):
        for fname in filenames:
            filelist[fname] = {'path': dirpath, 'filetype': filetype, 'fullname': dirpath+'/'+fname, 'fname': fname}

    return filelist



###########################################################################
def add_basenames_list(filelist):
    #input: filelist[fname] = {'path': path, 'filetype': filetype, 'fullname':fullname}
    #modified: filelist[fname] = {'path': path, 'filetype': filetype, 'fullname':fullname,
    #                             'filename', 'compression'}

    parsemask = miscutils.CU_PARSE_FILENAME | miscutils.CU_PARSE_COMPRESSION

    for fname in filelist:
        (filename, compress_ext) = miscutils.parse_fullname(fname, parsemask)
        filelist[fname]['filename'] = filename
        filelist[fname]['compression'] = compress_ext


###########################################################################
def list_missing_metadata(filemgmt, filelist):
    """ Return list of files from given set which are missing metadata """
    # filelist = list of file dicts

    miscutils.fwdebug(6, "REGISTER_FILES_DEBUG", "filelist=%s" % (filelist))

    origcount = len(filelist)

    # assume uncompressed and compressed files have same metadata
    # choosing either doesn't matter
    byfilename =  {}
    for fdict in filelist:
        byfilename[fdict['filename']] = fdict

    print "\tChecking which files already have metadata registered",
    starttime = time.time()
    havelist = filemgmt.file_has_metadata(byfilename.keys())
    endtime = time.time()
    print "(%0.2f secs)" % (endtime - starttime)

    missingfilenames = set(byfilename.keys()) - set(havelist)

    print "\t\t%0d file(s) already have metadata ingested" % len(havelist)
    print "\t\t%0d file(s) still to have metadata ingested" % len(missingfilenames)

    misslist = [ byfilename[f] for f in missingfilenames ]
    miscutils.fwdebug(6, "REGISTER_FILES_DEBUG", "misslist=%s" % (misslist))

    return misslist


###########################################################################
def list_missing_archive(filemgmt, filelist, archive_name, verbose):
    """ Return list of files from given list which are not listed in archive """

    print "\tChecking which files are already registered in archive",
    starttime = time.time()
    existing = filemgmt.is_file_in_archive(filelist, archive_name)
    endtime = time.time()
    print "(%0.2f secs)" % (endtime - starttime)

    missing_files = []
    for fdict in filelist:
        if (fdict['filename'], fdict['compression']) not in existing:
            missing_files.append(fdict)
            
    print "\t\t%0d file(s) already in archive" % len(existing)
    print "\t\t%0d file(s) still to be registered to archive" % len(missing_files)
    return missing_files



###########################################################################
def get_register_metadata_specs(ftype, filemgmt, verbose):
    """ Return dict/wcl describing metadata to gather for given filetype """

    metaspecs = OrderedDict()

    filetype_metadata = None
    file_header = None
    if (filemgmt.config is not None and
        'filetype_metadata' in filemgmt.config):
        filetype_metadata = filemgmt.config['filetype_metadata']
    else:
        print "Warning: no filetype_metadata"


    if (filetype_metadata is not None):
         # note:  When manually ingesting files generated externally to the framework, we do not want to modify the files (i.e., no updating/inserting headers
        #(reqmeta, optmeta, updatemeta) = metautils.create_file_metadata_dict(ftype, filetype_metadata, None, None)
        metaspecs = metautils.get_metadata_specs(ftype, filetype_metadata, None, None, False)

        #print 'keys = ', metaspecs.keys()
        #if updatemeta is not None:
        #    print "WARNING:  create_file_metadata_dict incorrectly returned values to update."
        #    print "\tContinuing but not updating these values."
        #    print "\tReport this to code developer."
        #    print "\t\t", updatemeta
        #    updatemeta = None

        if metaspecs is None:
            msg = "Error: Could not find metadata specs for filetype '%s'" % filetype
            print msg
            print "Minimum metadata specs for a filetype are defs for filetype and filename"
            miscutils.fwdie("Aborting", fmdefs.FM_EXIT_FAILURE)

        for key in metaspecs:
            #print "metaspecs key = ", key
            if type(metaspecs[key]) == dict or type(metaspecs[key]) == OrderedDict:
                if imetadefs.WCL_META_WCL in metaspecs[key]:
                    #print "deleting wcl from", key
                    del metaspecs[key][imetadefs.WCL_META_WCL]   # remove wcl requirements for manual file ingestion
                elif len(metaspecs[key]) == 0:
                    #print "deleting", key
                    del metaspecs[key]
        metaspecs['filetype'] = ftype
        #print metaspecs['fullname']

        if verbose >= 3:
            # print metaspecs to stdout
            print "\n\nmetaspecs = "
            import intgutils.wclutils as wclutils
            wclutils.write_wcl(metaspecs)
            print "\n\n"


    return metaspecs



###########################################################################
def save_file_info(filemgmt, task_id, ftype, metaspecs, filelist, save_md5sum, verbose):
    # filelist = list of file dicts

    # check which files already have metadata in database
    #     don't bother with updating existing data, as files should be immutable
    misslist = list_missing_metadata(filemgmt, filelist)

    #print "misslist = ", misslist
    if verbose >= 3:
        print "filenames for files which do not already have metadata registered:"
        for fdict in misslist:
            print "\t%s" % fdict['filename']

    if len(misslist) != 0:
        metaspecs['fullname'] = ','.join(sorted(afile['fullname'] for afile in misslist))

        print "\tGathering file metadata on %0d files...." % len(misslist),
        starttime = time.time()

        # call the function to get metadata from the fits files
        filemeta = {}
        filemeta = wraputils.get_file_metadata(metaspecs)   # get the metadata from the fits files
        if verbose >= 3:
            print "\n\noutput wcl"
            if filemeta is not None:
                import intgutils.wclutils as wclutils
                wclutils.write_wcl(filemeta)
            else:
                print "None"

        if filemeta is not None:
            # add filenames and filetypes to metadata
            for fdict in filemeta.values():
                fdict['filename'] = miscutils.parse_fullname(fdict['fullname'], miscutils.CU_PARSE_FILENAME)
                fdict['filetype'] = ftype
        else:
            print "Creating filename/filetype metadata info"
            filemeta = {}
            fcnt = 0
            for fdict in misslist:
                miscutils.fwdebug(6, "REGISTER_FILES_DEBUG", "file=%s" % (fdict))
                filemeta['file_%s' % fcnt] = {'filename': miscutils.parse_fullname(fdict['fullname'], miscutils.CU_PARSE_FILENAME),
                                              'filetype': ftype}
                fcnt += 1
        endtime = time.time()
        print "DONE (%0.2f secs)" % (endtime-starttime)

        # get disk information for artifacts
        print "\tGetting disk information on %s files..." % len(filemeta),
        starttime = time.time()
        artifacts = []
        for fdict in filemeta.values():
            artifacts.append(diskutils.get_single_file_disk_info(fdict['fullname'],
                                                                 save_md5sum=save_md5sum,
                                                                 archive_root=None))
        endtime = time.time()
        print "DONE (%0.2f secs)" % (endtime-starttime)

        # create provenance showing was registered by this program
        wgblist = []
        for fdict in artifacts:
            filename = fdict['filename']
            if fdict['compression'] is not None:
                 filename += fdict['compression']
            wgblist.append(filename)
        file_prov = {'was_generated_by': {'exec_1': ','.join(sorted(wgblist))}}
        prov_task_ids = {'exec_1': task_id}

        print "\tCalling ingest_file_metadata on %s files..." % len(filemeta),
        starttime = time.time()
        try:
            filemgmt.save_file_info(artifacts, filemeta, file_prov, prov_task_ids)
            endtime=time.time()
        except Exception as err:
            endtime=time.time()
            print "ERROR (%0.2f secs)" % (endtime-starttime)
            print "\n\n\nError: %s" % err
            print "Rerun using --outcfg <outfile> to see config from DB, esp filetype_metadata"
            print "---------- filemeta to ingest:"
            import intgutils.wclutils as wclutils
            wclutils.write_wcl(filemeta)
            print "----------"
            raise
        print "DONE (%0.2f secs)" % (endtime-starttime)


###########################################################################
def save_archive_location(filemgmt, filelist, archive_name, verbose):
    """ save location in archive """

    # check which files already are in archive
    missing_files = list_missing_archive(filemgmt, filelist, archive_name, verbose)

    # create input list of files that need to be registered in archive
    if len(missing_files) > 0:
        print "\tRegistering %s file(s) in archive..." % len(missing_files),
        starttime = time.time()
        problemfiles = filemgmt.register_file_in_archive(missing_files, archive_name)
        endtime = time.time()
        if problemfiles is not None and len(problemfiles) > 0:
            print "ERROR (%0.2f secs)" % (endtime-starttime)
            print "\n\n\nError: putting %0d files into archive" % len(problemfiles)
            for file in problemfiles:
                print file, problemfiles[file]
                sys.exit(1)
        print "DONE (%0.2f secs)" % (endtime-starttime)



###########################################################################
def process_files(filelist, filemgmt, task_id, archive_name, save_md5sum, commit, verbose):
    """ Ingests file metadata for all files in filelist """
    # filelist[fullname] = {'path': path, 'filetype': filetype, 'fullname':fullname,
    #                       'filename', 'compression'}

    print "\nProcessing %0d files" % (len(filelist))
    miscutils.fwdebug(6, "REGISTER_FILES_DEBUG", "filelist=%s" % (filelist))

    # group by filetype
    byfiletype = {}
    for fdict in filelist.values():
        filetype = fdict['filetype']
        if filetype not in byfiletype:
            byfiletype[filetype] = []
        byfiletype[filetype].append(fdict)
    #print byfiletype
    miscutils.fwdebug(6, "REGISTER_FILES_DEBUG", "byfiletype=%s" % (byfiletype))

    # work in sets defined by filetype
    for ftype in sorted(byfiletype.keys()):
        print "\n%s:" % ftype
        print "\tTotal: %s file(s) of this type" % len(byfiletype[ftype])
        if verbose >= 3:
            print "filenames for files for filetype %s:" % ftype
            for f in byfiletype[ftype]:
                print "\t%s" % f

        metaspecs = get_register_metadata_specs(ftype, filemgmt, verbose)
        save_file_info(filemgmt, task_id, ftype, metaspecs, byfiletype[ftype], save_md5sum, verbose)
        save_archive_location(filemgmt, byfiletype[ftype], archive_name, verbose)

        if commit:
            filemgmt.commit()
            




###########################################################################
def main(args):
    starttime = time.time()

    parser = argparse.ArgumentParser(description='Ingest metadata for files generated outside DESDM framework')
    parser.add_argument('--des_services', action='store', help='')
    parser.add_argument('--section', action='store', help='Must be specified if not set in environment')
    parser.add_argument('--provmsg', action='store', required=True)
    parser.add_argument('--config', action='store')
    parser.add_argument('--outcfg', action='store')
    parser.add_argument('--classmgmt', action='store')
    parser.add_argument('--classutils', action='store')

    parser.add_argument('--no-commit', action='store_true', default=False)
    parser.add_argument('--list', action='store', help='format:  fullname, filetype')
    parser.add_argument('--archive', action='store', help='single value')
    parser.add_argument('--filetype', action='store', help='single value, must also specify search path')
    parser.add_argument('--path', action='store', help='single value, must also specify filetype')
    parser.add_argument('--skip-md5sum', action='store_true', default=False)
    parser.add_argument('--verbose', action='store', default=1)
    parser.add_argument('--version', action='store_true', default=False)

    args = vars(parser.parse_args())   # convert to dict

    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)  # turn off buffering of stdout

    m = re.search('\$Rev:\s+(\d+)\s+\$', VERSION)
    print '\nUsing revision %s of %s\n' % (m.group(1), os.path.basename(sys.argv[0]))

    if args['version']:
        return 0

    if not args['archive']:
        print "Error: must specify archive\n"
        parser.print_help()
        return 1
    archive = args['archive']

    if args['filetype'] and ',' in args['filetype']:
        print "Error: filetype must be single value\n"
        parser.print_help()
        return 1

    if args['path'] and ',' in args['path']:
        print "Error: path must be single value\n"
        parser.print_help()
        return 1

    if args['filetype'] and args['path'] is None:
        print "Error: must specify path if using filetype\n"
        parser.print_help()
        return 1

    if args['filetype'] is None and args['path']:
        print "Error: must specify filetype if using path\n"
        parser.print_help()
        return 1

    if not args['filetype'] and not args['list']:
        print "Error: must specify either list or filetype+path\n"
        parser.print_help()
        return 1

    save_md5sum = not args['skip_md5sum']
    commit = not args['no_commit']

    verbose = 1
    if 'verbose' in args:
        verbose = args['verbose']


    # figure out which python class to use for filemgmt
    filemgmt_class = None
    if args['classmgmt']:
        filemgmt_class = args['classmgmt']
    elif args['config']:
        if args['config'] is not None:
            import intgutils.wclutils as wclutils
            with open(args['config'], 'r') as fh:
                config = wclutils.read_wcl(fh)
        if archive in config['archive']:
            filemgmt_class = config['archive'][archive]['filemgmt']
        else:
            miscutils.fwdie("Invalid archive name (%s)" % archive, 1)
    else:
        import despydmdb.desdmdbi as desdmdbi
        with desdmdbi.DesDmDbi(args['des_services'], args['section']) as dbh:
            curs = dbh.cursor()
            sql = "select filemgmt from ops_archive where name='%s'" % archive
            curs.execute(sql)
            rows = curs.fetchall()
            if len(rows) > 0:
                filemgmt_class = rows[0][0]
            else:
                miscutils.fwdie("Invalid archive name (%s)" % archive, 1)

    if filemgmt_class is None or '.' not in filemgmt_class:
        print "Error: Invalid filemgmt class name (%s)" % filemgmt_class
        print "\tMake sure it contains at least 1 period."
        miscutils.fwdie("Invalid filemgmt class name", 1)


    # dynamically load class for filemgmt
    filemgmt = None
    filemgmt_class = miscutils.dynamically_load_class(filemgmt_class)
    try:
        filemgmt = filemgmt_class(argv=args)
    except Exception as err:
        print "ERROR\nError: creating filemgmt object\n%s" % err
        raise

    if not filemgmt.is_valid_archive(archive):
        miscutils.fwdie("Invalid archive name (%s)" % archive, 1)



    if args['outcfg'] is not None:
        import intgutils.wclutils as wclutils
        with open(args['outcfg'], 'w') as fh:
           wclutils.write_wcl(filemgmt.config, fh)

    print "Creating list of files to register...",
    starttime = time.time()
    if args['filetype'] is not None:
        if not filemgmt.is_valid_filetype(args['filetype']):
            miscutils.fwdie("Error:  Invalid filetype (%s)" % args['filetype'], 1)
        filelist = get_list_filenames(args['path'], args['filetype'])
        miscutils.fwdebug(6, "REGISTER_FILES_DEBUG", "after get_list_filenames:  filelist=%s" % (filelist))
    elif args['list'] is not None:
        filelist = parse_provided_list(args['list'])
    add_basenames_list(filelist)
    endtime = time.time()
    miscutils.fwdebug(3, "REGISTER_FILES_DEBUG", "after add_basenames_list:  filelist=%s" % (filelist))
    print "DONE (%0.2f secs)" % (endtime - starttime)
    print "\t%s files in list" % len(filelist)
    
    ###
    print "Creating task and entry in file_registration...",
    starttime = time.time()
    task_id = filemgmt.create_task(name='register_files', info_table='file_registration',
                    parent_task_id = None, root_task_id = None, i_am_root = True,
                    label = None, do_begin = True, do_commit = commit)

    # save provenance message
    save_register_info(filemgmt, task_id, args['provmsg'], commit)
    endtime = time.time()
    print "DONE (%0.2f secs)" % (endtime - starttime)


    print """\nReminder:
\tFor purposes of file metadata, uncompressed and compressed
\tfiles are treated as same file (no checking is done).
\tBut when tracking file locations within archive, 
\tthey are tracked as 2 independent files.\n"""
    try:
        process_files(filelist, filemgmt, task_id, archive, save_md5sum, commit, verbose)
        filemgmt.end_task(task_id, fmdefs.FM_EXIT_SUCCESS, commit)
        if not commit:
            print "Skipping commit"
    except:
        filemgmt.end_task(task_id, fmdefs.FM_EXIT_FAILURE, commit)
        raise

    endtime = time.time()
    print "\n\nTotal time with %s files: %0.2f secs" % (len(filelist), endtime-starttime)


if __name__ == '__main__':
    sys.exit(main(sys.argv))

