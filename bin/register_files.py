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
from collections import OrderedDict
import wrappers.WrapperUtils as wraputils
import intgutils.wclutils as wclutils
#import processingfw.pfwdb as pfwdb

from filemgmt.filemgmt_defs import *
from filemgmt.errors import *
from coreutils.miscutils import *
#import processingfw.pfwutils as pfwutils
#import processingfw.errors

VERSION = '$Rev$'

###########################################################################
def parse_provided_list(listname):
    """ create dictionary of files from list in file """

    cwd = os.getcwd()

    filelist = {}
    try:
        with open(listname, "r") as fh:
            for line in fh:
                (fullname, filetype) = fwsplit(line, ',')
                if fullname[0] != '/':
                    fullname = cwd + '/' + fullname

                if not os.path.exists(fullname):
                    fwdie("Error:   could not find file to register:  %s" % fullname, 1)
                    
                (path, fname) = os.path.split(fullname)
                if fname in filelist:
                    print "Error:   Found duplicate filenames in list:  %s"
                    print "\t%s" % fullname
                    print "\t%s" % filelist[fname]['fullname']
                    fwdie("Error:   Found duplicate filenames in list:  %s" % fname, 1)
                filelist[fname] = {'path': path, 'filetype': filetype, 'fullname':fullname, 'fname':fname}
    except Exception as err:
        fwdie("Error: Problems reading file '%s': %s" % (listname, err), 1)
        
    return filelist


###########################################################################
def get_list_filenames(ingestpath, filetype):
    """ create a dictionary of files in given path """

    if ingestpath[0] != '/':
        cwd = os.getenv('PWD')  # don't use getcwd as it canonicallizes path
                                # which is not what we want for links internal to archive
        ingestpath = cwd + '/' + ingestpath

    if not os.path.exists(ingestpath):
        fwdie("Error:   could not find ingestpath:  %s" % ingestpath, 1)

    filelist = {}
    for (dirpath, dirnames, filenames) in os.walk(ingestpath):
        for fname in filenames:
            filelist[fname] = {'path': dirpath, 'filetype': filetype, 'fullname': dirpath+'/'+fname, 'fname': fname}

    return filelist



###########################################################################
def add_basenames_list(filelist):
    #filelist[fname] = {'path': path, 'filetype': filetype, 'fullname':fullname}
   
    for fname in filelist:
        (filename, compress_ext) = parse_fullname(fname, 3)
        filelist[fname]['filename'] = filename
        filelist[fname]['compression'] = compress_ext


###########################################################################
def list_missing_metadata(filelist, fnames, filemgmt):
    """ Return list of files from given set which are missing metadata """

    origcount = len(fnames)


    # assume uncompressed and compressed files have same metadata
    # choosing either doesn't matter
    byfilename =  {}
    for f in fnames:
        byfilename[filelist[f]['filename']] = filelist[f]

    print "\tChecking which files already have metadata registered"
    havelist = filemgmt.file_has_metadata(byfilename.keys())

    missingfilenames = set(byfilename.keys()) - set(havelist)

    print "\t\t%0d file(s) already have metadata ingested" % len(havelist)
    print "\t\t%0d file(s) still to have metadata ingested" % len(missingfilenames)

    fnames = [ byfilename[f]['fname'] for f in missingfilenames ]

    return fnames


def list_missing_archive(filelist, fnames, filemgmt, args):
    """ Return list of files from given list which are not listed in archive """

    print "\tChecking which files are already registered in archive"
    existing_fnames = filemgmt.is_file_in_archive(fnames, filelist, args)
    missing_fnames = set(fnames) - set(existing_fnames)
    print "\t\t%0d file(s) already in archive" % len(existing_fnames)
    print "\t\t%0d file(s) still to be registered to archive" % len(missing_fnames)
    return missing_fnames





###########################################################################
def list_not_in_archive(filenames, archive, dbh):
    """ Return list of files from given set which are not in archive """

    origcount = len(filenames)

    print "\tChecking if files are already in archive"
    dbq = "select filename from file_archive_info where archive_name='%s' and filename in ('%s')" % \
                                                                (archive, "','".join(filenames))

    curs = dbh.cursor()
    curs.execute(dbq)
    dblist = []
    for row in curs:
        dblist.append(row[0])

    filenames = set(filenames) - set(dblist)
    print "\t\t%0d files already in archive" % len(dblist)
    print "\t\t%0d files still to be copied to archive" % len(filenames)

    return filenames



###########################################################################
#def get_metadata_specs(ftype, config):
#    """ Return dict/wcl describing metadata to gather for given filetype """
#
#    metaspecs = OrderedDict()
#
#    # note:  When manually ingesting files generated externally to the framework, we do not want to modify the files (i.e., no updating/inserting headers
#    (reqmeta, optmeta, updatemeta) = pfwutils.create_file_metadata_dict(ftype, config['filetype_metadata'], None, None)
#
#    if reqmeta:
#        metaspecs[IW_REQ_META] = reqmeta
#
#    if optmeta:
#        metaspecs[IW_OPT_META] = optmeta
#
#    #print 'keys = ', metaspecs.keys()
#    if updatemeta is not None:
#        print "WARNING:  create_file_metadata_dict incorrectly returned values to update."
#        print "\tContinuing but not updating these values."
#        print "\tReport this to code developer."
#        print "\t\t", updatemeta
#        updatemeta = None
#
#    return metaspecs
#


###########################################################################
def process_files(filelist, filemgmt, args):
    """ Ingests file metadata for all files in filelist """ 

    print "\nProcessing %0d files" % (len(filelist))
    # group by filetype
    byfiletype = {}
    for f in filelist:
        filetype = filelist[f]['filetype']
        if filetype not in byfiletype:
            byfiletype[filetype] = []
        byfiletype[filetype].append(f)
    #print byfiletype

    for ftype in sorted(byfiletype.keys()):
        print "\n%s:" % ftype
        print "\tTotal: %s file(s) of this type" % len(byfiletype[ftype])

        # check which files already have metadata in database
        #     don't bother with updating existing data, as files should be immutable
        filenames = list_missing_metadata(filelist, byfiletype[ftype], filemgmt)
        #print "filenames = ", filenames

        if len(filenames) != 0:
            # create input list of files that need to have metadata gathered
            insfilelist = {}
            for f in filenames:
                insfilelist[f] = filelist[f]

            metaspecs = filemgmt.get_metadata_specs(ftype)
            for key in metaspecs:
                #print "metaspecs key = ", key
                if type(metaspecs[key]) == dict or type(metaspecs[key]) == OrderedDict:
                    if WCL_META_WCL in metaspecs[key]:
                        #print "deleting wcl from", key
                        del metaspecs[key][WCL_META_WCL]   # remove wcl requirements for manual file ingestion
                    elif len(metaspecs[key]) == 0:
                        #print "deleting", key
                        del metaspecs[key]
            metaspecs['filetype'] = ftype
            metaspecs['fullname'] = ','.join(sorted(afile['fullname'] for afile in insfilelist.values()))
            #print metaspecs['fullname']

            print "\tGathering file metadata on %0d files...." % len(insfilelist), 
            #print "\n\nmetaspecs = "
            #import intgutils.wclutils as wclutils
            #wclutils.write_wcl(metaspecs)
            #print "\n\n"

     
            filemeta = {}
            filemeta = wraputils.get_file_metadata(metaspecs)   # get the metadata from the fits files
            #print "\n\noutput wcl"
            #wclutils.write_wcl(filemeta)
    
            if filemeta is not None:
                # add filenames and filetypes to metadata
                for fdict in filemeta.values():
                    fdict['filename'] = wclutils.getFilename(fdict['fullname'])
                    fdict['filetype'] = ftype
            else:
                print "Creating filename/filetype metadata info"
                filemeta = {}
                fcnt = 0
                for f in insfilelist:
                    filemeta['file_%s' % fcnt] = {'filename': wclutils.getFilename(f), 
                                                  'filetype': ftype}
                    fcnt += 1

            print "DONE"
         
            if 'noingest' not in args or not args['noingest']:
                print "\tCalling ingest_file_metadata on %s files..." % len(filemeta), 
                try:
                    filemgmt.ingest_file_metadata(filemeta)
                except FileMetadataIngestError as err:
                    print "\n\n\nError: %s" % err
                    print "Rerun using --outcfg <outfile> to see config from DB, esp filetype_metadata"
                    print "---------- filemeta to ingest:"
                    wclutils.write_wcl(filemeta)
                    print "----------"
                    raise

                print "DONE"


        # check which files already are in archive
        new_fnames = list_missing_archive(filelist, byfiletype[ftype], filemgmt, args)

        # create input list of files that need to have metadata gathered
        if len(new_fnames) > 0:
            insfilelist = {}
            for f in new_fnames:
                insfilelist[f] = filelist[f]
                insfilelist[f]['filesize'] = os.path.getsize(filelist[f]['fullname'])

            print "\tRegistering %s file(s) in archive..." % len(new_fnames),
            problemfiles = filemgmt.register_file_in_archive(insfilelist, args)
            if problemfiles is not None and len(problemfiles) > 0:
                print "ERROR\n\n\nError: putting %0d files into archive" % len(problemfiles)
                for file in problemfiles:
                    print file, problemfiles[file]
                    sys.exit(1)
            else:
                filemgmt.commit()
            print "DONE"

     
###########################################################################
def main(args):
    parser = argparse.ArgumentParser(description='Ingest metadata for files generated outside DESDM framework')
    parser.add_argument('--config', action='store')
    parser.add_argument('--outcfg', action='store')
    parser.add_argument('--classmgmt', action='store')
    parser.add_argument('--classutils', action='store')
    parser.add_argument('--noingest', action='store_true', default=False)
    parser.add_argument('--list', action='store', help='format:  fullname, filetype, archivepath')
    parser.add_argument('--archive', action='store', help='single value')
    parser.add_argument('--filetype', action='store', help='single value, must also specify search path')
    parser.add_argument('--path', action='store', help='single value, must also specify filetype')
    parser.add_argument('--version', action='store_true', default=False)

    args = vars(parser.parse_args())   # convert to dict

    
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)  # turn off buffering of stdout

    m = re.search('\$Rev:\s+(\d+)\s+\$', VERSION)
    print 'Using revision %s of %s\n' % (m.group(1), os.path.basename(sys.argv[0]))
    
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


    # figure out which python class to use for filemgmt
    filemgmt_class = None
    if args['classmgmt']:
        filemgmt_class = args['classmgmt']
    elif args['config']:
        if args['config'] is not None:
            with open(args['config'], 'w') as fh:
                config = wclutils.read_wcl(fh)
        if archive in config['archive']:
            filemgmt_class = config['archive'][archive]['filemgmt'] 
        else:
            fwdie("Invalid archive name (%s)" % archive, 1)
    else:
        import coreutils.desdbi
        with coreutils.desdbi.DesDbi() as dbh:
            curs = dbh.cursor()
            sql = "select filemgmt from ops_archive where name='%s'" % archive
            curs.execute(sql)
            rows = curs.fetchall()
            if len(rows) > 0:
                filemgmt_class = rows[0][0]
            else:
                fwdie("Invalid archive name (%s)" % archive, 1)

    if filemgmt_class is None or '.' not in filemgmt_class:
        print "Error: Invalid filemgmt class name (%s)" % filemgmt_class
        print "\tMake sure it contains at least 1 period."
        fwdie("Invalid filemgmt class name", 1)


    # dynamically load class for filemgmt
    filemgmt = None
    filemgmt_class = dynamically_load_class(filemgmt_class)
    try:
        filemgmt = filemgmt_class(argv=args)
    except Exception as err:
        print "ERROR\nError: creating filemgmt object\n%s" % err
        raise

    if not filemgmt.is_valid_archive(archive):
        fwdie("Invalid archive name (%s)" % archive, 1)


    if args['filetype'] is not None:
        if not filemgmt.is_valid_filetype(args['filetype']):
            fwdie("Error:  Invalid filetype (%s)" % args['filetype'], 1)
        filelist = get_list_filenames(args['path'], args['filetype'])
    elif args['list'] is not None:
        filelist = parse_provided_list(args['list'])
    add_basenames_list(filelist)


    process_files(filelist, filemgmt, args)
    
if __name__ == '__main__':
    sys.exit(main(sys.argv))

