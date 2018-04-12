#!/usr/bin/env python

""" Define a class to do file management tasks without DB.
"""

import os
import sys
import socket
import argparse
from collections import OrderedDict

import despymisc.miscutils as miscutils
import filemgmt.filemgmt_defs as fmdefs
import filemgmt.errors as fmerrs
import filemgmt.utils as fmutils


class FileMgmtNoDB ():
    """
    """

    @staticmethod
    def requested_config_vals():
        return {'archive': 'req', fmdefs.FILE_HEADER_INFO: 'req', 'filetype_metadata': 'req'}

    def __init__(self, config=None, argv=None):
        self.config = config
        self.argv = argv

    def get_list_filenames(self, args):
        # args is an array of "command-line" args possibly with keys for query
        # returns python list of filenames

        raise Exception("NoDB filemgmt does not support this functionality")

    def register_file_in_archive(self, filelist, args):
        # with no db, don't need to register
        miscutils.fwdebug_print("Nothing to do")
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

    def save_file_info(self, artifacts, metadata, prov, execids):
        pass

    def ingest_file_metadata(self, filemeta):
        pass

    def is_valid_filetype(self, ftype):
        if ftype.lower() in self.config[fmdefs.FILETYPE_METADATA]:
            return True
        else:
            return False

    def is_valid_archive(self, arname):
        if arname.lower() in self.config['archive']:
            return True
        else:
            return False

    def get_file_location(self, filelist, arname, compress_order=fmdefs.FM_PREFER_COMPRESSED):
        fileinfo = self.get_file_archive_info(filelist, arname, compress_order)
        rel_filenames = {}
        for f, finfo in list(fileinfo.items()):
            rel_filenames[f] = finfo['rel_filename']
        return rel_filenames

    # compression = compressed_only, uncompressed_only, prefer uncompressed, prefer compressed, either (treated as prefer compressed)
    def get_file_archive_info(self, filelist, arname, compress_order=fmdefs.FM_PREFER_COMPRESSED):

        # sanity checks
        if 'archive' not in self.config:
            miscutils.fwdie('Error: Missing archive section in config', 1)

        if arname not in self.config['archive']:
            miscutils.fwdie('Error: Invalid archive name (%s)' % arname, 1)

        if 'root' not in self.config['archive'][arname]:
            miscutils.fwdie('Error: Missing root in archive def (%s)' % self.config['archive'][arname], 1)

        if not isinstance(compress_order, list):
            miscutils.fwdie(
                'Error:  Invalid compress_order.  It must be a list of compression extensions (including None)', 1)

        # walk archive to get all files
        fullnames = {}
        for p in compress_order:
            fullnames[p] = {}

        root = self.config['archive'][arname]['root']
        root = root.rstrip("/")  # canonicalize - remove trailing / to ensure

        for (dirpath, dirnames, filenames) in os.walk(root, followlinks=True):
            for fname in filenames:
                d = {}
                (d['filename'], d['compression']) = miscutils.parse_fullname(fname, 3)
                d['filesize'] = os.path.getsize("%s/%s" % (dirpath, fname))
                d['path'] = dirpath[len(root)+1:]
                if d['compression'] is None:
                    compext = ""
                else:
                    compext = d['compression']
                d['rel_filename'] = "%s/%s%s" % (d['path'], d['filename'], compext)
                fullnames[d['compression']][d['filename']] = d

        print("uncompressed:", len(fullnames[None]))
        print("compressed:", len(fullnames['.fz']))

        # go through given list of filenames and find archive location and compreesion
        archiveinfo = {}
        for name in filelist:
            #print name
            for p in compress_order:    # follow compression preference
                #print "p = ", p
                if name in fullnames[p]:
                    archiveinfo[name] = fullnames[p][name]
                    break

        print("archiveinfo = ", archiveinfo)
        return archiveinfo

    # compression = compressed_only, uncompressed_only, prefer uncompressed, prefer compressed, either (treated as prefer compressed)
    def get_file_archive_info_path(self, path, arname, compress_order=fmdefs.FM_PREFER_COMPRESSED):

        # sanity checks
        if 'archive' not in self.config:
            miscutils.fwdie('Error: Missing archive section in config', 1)

        if arname not in self.config['archive']:
            miscutils.fwdie('Error: Invalid archive name (%s)' % arname, 1)

        if 'root' not in self.config['archive'][arname]:
            miscutils.fwdie('Error: Missing root in archive def (%s)' % self.config['archive'][arname], 1)

        if not isinstance(compress_order, list):
            miscutils.fwdie(
                'Error:  Invalid compress_order.  It must be a list of compression extensions (including None)', 1)

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
                (d['filename'], d['compression']) = miscutils.parse_fullname(fname, 3)
                d['filesize'] = os.path.getsize("%s/%s" % (dirpath, fname))
                d['path'] = dirpath[len(root)+1:]
                if d['compression'] is None:
                    compext = ""
                else:
                    compext = d['compression']
                d['rel_filename'] = "%s/%s%s" % (d['path'], d['filename'], compext)
                fullnames[d['compression']][d['filename']] = d
                list_by_name[d['filename']] = True

        print("uncompressed:", len(fullnames[None]))
        print("compressed:", len(fullnames['.fz']))

        # go through given list of filenames and find archive location and compreesion
        archiveinfo = {}
        for name in list(list_by_name.keys()):
            #print name
            for p in compress_order:    # follow compression preference
                #print "p = ", p
                if name in fullnames[p]:
                    archiveinfo[name] = fullnames[p][name]
                    break

        print("archiveinfo = ", archiveinfo)
        return archiveinfo

    def commit(self):
        miscutils.fwdebug_print("Nothing to do")
