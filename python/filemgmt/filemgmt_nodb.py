#!/usr/bin/env python

# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
    Define a database utility class extending coreutils.DesDbi

    Developed at:
    The National Center for Supercomputing Applications (NCSA).

    Copyright (C) 2012 Board of Trustees of the University of Illinois.
    All rights reserved.
"""

__version__ = "$Rev$"

import os
import sys
import socket
import argparse
from collections import OrderedDict

from coreutils.miscutils import *
from filemgmt.filemgmt_defs import *
from filemgmt.errors import *
import filemgmt.utils as fmutils


class FileMgmtNoDB ():
    """
    """

    @staticmethod
    def requested_config_vals():
        return {'archive':'req', FILE_HEADER_INFO:'req', 'filetype_metadata':'req'}

    def __init__ (self, config=None, argv=None):
        self.config = config
        self.argv = argv

    def get_metadata_specs(self, ftype, sectlabel=None, updatefits=False):
        """ Return dict/wcl describing metadata to gather for given filetype """

        print "ftype =", ftype
        metaspecs = OrderedDict()

        # note:  When manually ingesting files generated externally to the framework, we do not want to modify the files (i.e., no updating/inserting headers
        
        file_header_info = None
        if updatefits:
            file_header_info = self.config[FILE_HEADER_INFO]

        (reqmeta, optmeta, updatemeta) = fmutils.create_file_metadata_dict(ftype, self.config[FILETYPE_METADATA], sectlabel, file_header_info)

        #print "reqmeta =", reqmeta
        #print "======================================================================"
        #print "optmeta =", optmeta
        #print "======================================================================"
        #print "updatemeta =", updatemeta
        #print "======================================================================"
        #sys.exit(1)

        if reqmeta:
            metaspecs[WCL_REQ_META] = OrderedDict()

            # convert names from specs to wcl
            valspecs = [META_HEADERS, META_COMPUTE, META_WCL]
            wclspecs = [WCL_META_HEADERS, WCL_META_COMPUTE, WCL_META_WCL]
            for i in range(len(valspecs)):
                if valspecs[i] in reqmeta:
                    metaspecs[WCL_REQ_META][wclspecs[i]] = reqmeta[valspecs[i]]

        if optmeta:
            metaspecs[WCL_OPT_META] = OrderedDict()

            # convert names from specs to wcl
            valspecs = [META_HEADERS, META_COMPUTE, META_WCL]
            wclspecs = [WCL_META_HEADERS, WCL_META_COMPUTE, WCL_META_WCL]
            for i in range(len(valspecs)):
                if valspecs[i] in optmeta:
                    metaspecs[WCL_OPT_META][wclspecs[i]] = optmeta[valspecs[i]]

        #print 'keys = ', metaspecs.keys()
        if updatefits:
            if updatemeta:
                updatemeta[WCL_UPDATE_WHICH_HEAD] = '0'  # framework always updates primary header
                metaspecs[WCL_UPDATE_HEAD_PREFIX+'0'] = updatemeta               
        elif updatemeta is not None:
            print "WARNING:  create_file_metadata_dict incorrectly returned values to update."
            print "\tContinuing but not updating these values."
            print "\tReport this to code developer."
            print "\t\t", updatemeta
            updatemeta = None

        # return None if no metaspecs
        if len(metaspecs) == 0:
            metaspecs = None

        return metaspecs


    def get_list_filenames(self, args):
        # args is an array of "command-line" args possibly with keys for query
        # returns python list of filenames

        raise Exception("NoDB filemgmt does not support this functionality")
            

    def register_file_in_archive(self, filelist, args):
        # with no db, don't need to register
        fwdebug(0, "FILEMGMT_NODB_DEBUG", "Nothing to do")
        return {}


    def file_has_metadata(self, filenames):
        return filenames

    def is_file_in_archive(self, fnames, filelist, args):
        archivename = args['archive']
        archivedict = self.config['archive'][archivename]
        archiveroot = os.path.realpath(archivedict['root'])

        in_archive = []
        for f in fnames:
            if os.path.exists(archiveroot + '/' + filelist['path'] + '/' + f):
                in_archive.append(f)
        return in_archive
                

    def ingest_file_metadata(self, filemeta):
        pass

    def is_valid_filetype(self, ftype):
        if ftype.lower() in self.config[FILETYPE_METADATA]:
            return True
        else:
            return False

    def is_valid_archive(self, arname):
        if arname.lower() in self.config['archive']:
            return True
        else:
            return False


    def get_file_location(self, filelist, arname, compress_order=FM_PREFER_COMPRESSED):
        fileinfo = self.get_file_archive_info(filelist, arname, compress_order)
        rel_filenames = {}
        for f, finfo in fileinfo.items():
            rel_filenames[f] = finfo['rel_filename']
        return rel_filenames


    # compression = compressed_only, uncompressed_only, prefer uncompressed, prefer compressed, either (treated as prefer compressed)
    def get_file_archive_info(self, filelist, arname, compress_order=FM_PREFER_COMPRESSED):

        # sanity checks
        if 'archive' not in self.config:
            fwdie('Error: Missing archive section in config', 1)

        if arname not in self.config['archive']:
            fwdie('Error: Invalid archive name (%s)' % arname, 1)

        if 'root' not in self.config['archive'][arname]:
            fwdie('Error: Missing root in archive def (%s)' % self.config['archive'][arname], 1)

        if not isinstance(compress_order, list):
            fwdie('Error:  Invalid compress_order.  It must be a list of compression extensions (including None)')

        # walk archive to get all files
        fullnames = {}
        for p in compress_order:
            fullnames[p] = {}

        root = self.config['archive'][arname]['root']
        root = root.rstrip("/")  # canonicalize - remove trailing / to ensure 
        
        for (dirpath, dirnames, filenames) in os.walk(root, followlinks=True):
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
                fullnames[d['compression']][d['filename']] = d
                
        print "uncompressed:", len(fullnames[None])
        print "compressed:", len(fullnames['.fz'])

        # go through given list of filenames and find archive location and compreesion
        archiveinfo = {}
        for name in filelist:
            #print name
            for p in compress_order:    # follow compression preference
                #print "p = ", p
                if name in fullnames[p]:
                    archiveinfo[name] = fullnames[p][name]
                    break

        print "archiveinfo = ", archiveinfo
        return archiveinfo



    # compression = compressed_only, uncompressed_only, prefer uncompressed, prefer compressed, either (treated as prefer compressed)
    def get_file_archive_info_path(self, path, arname, compress_order=FM_PREFER_COMPRESSED):

        # sanity checks
        if 'archive' not in self.config:
            fwdie('Error: Missing archive section in config', 1)

        if arname not in self.config['archive']:
            fwdie('Error: Invalid archive name (%s)' % arname, 1)

        if 'root' not in self.config['archive'][arname]:
            fwdie('Error: Missing root in archive def (%s)' % self.config['archive'][arname], 1)

        if not isinstance(compress_order, list):
            fwdie('Error:  Invalid compress_order.  It must be a list of compression extensions (including None)')

        # walk archive to get all files
        fullnames = {}
        for p in compress_order:
            fullnames[p] = {}

        root = self.config['archive'][arname]['root']
        root = root.rstrip("/")  # canonicalize - remove trailing / to ensure 
        
        list_by_name = {}
        for (dirpath, dirnames, filenames) in os.walk(root + '/' + path):
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
                fullnames[d['compression']][d['filename']] = d
                list_by_name[d['filename']] = True
                
        print "uncompressed:", len(fullnames[None])
        print "compressed:", len(fullnames['.fz'])

        # go through given list of filenames and find archive location and compreesion
        archiveinfo = {}
        for name in list_by_name.keys():
            #print name
            for p in compress_order:    # follow compression preference
                #print "p = ", p
                if name in fullnames[p]:
                    archiveinfo[name] = fullnames[p][name]
                    break

        print "archiveinfo = ", archiveinfo
        return archiveinfo

    def commit(self):
        fwdebug(0, "FILEMGMT_NODB_DEBUG", "Nothing to do")
        