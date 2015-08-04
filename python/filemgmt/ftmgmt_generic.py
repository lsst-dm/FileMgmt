#!/usr/bin/env python

# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
Generic filetype management class used to do filetype specific tasks
     such as metadata and content ingestion
"""

__version__ = "$Rev$"

from collections import OrderedDict

import despymisc.miscutils as miscutils
import despydmdb.dmdb_defs as dmdbdefs

class FtMgmtGeneric(object):
    """  Base/generic class for managing a filetype (get metadata, update metadata, etc) """

    ######################################################################
    def __init__(self, filetype, dbh, config):
        """ Initialize object """
        # config must have filetype_metadata and file_header_info
        self.filetype = filetype
        self.dbh = dbh
        self.config = config


    ######################################################################
    def has_metadata_ingested(self, listfullnames):
        """ Check if file has row in genfile table """

        # assume uncompressed and compressed files have same metadata
        # choosing either doesn't matter
        byfilename = {}
        for fname in listfullnames:
            filename = miscutils.parse_fullname(fname, miscutils.CU_PARSE_FILENAME)
            byfilename[filename] = fname

        self.dbh.empty_gtt(dmdbdefs.DB_GTT_FILENAME)
        self.dbh.load_filename_gtt(byfilename.keys())

        metadata_table = self.config['filetype_metadata'][self.filetype]['metadata_table']
        dbq = "select m.filename from %s m, %s g where m.filename=g.filename" % \
                 (metadata_table, dmdbdefs.DB_GTT_FILENAME)
        curs = self.dbh.cursor()
        curs.execute(dbq)

        results = {}
        for row in curs:
            results[byfilename[row[0]]] = True

        for fname in listfullnames:
            if fname not in results:
                results[fname] = False    

        self.dbh.empty_gtt(dmdbdefs.DB_GTT_FILENAME)

        return results

    ######################################################################
    def has_contents_ingested(self, listfullnames):
        """ Check if file has contents ingested """

        # 0 contents to ingest, so true
        results = {}
        for fname in listfullnames:
            results[fname] = True

        return results

    ######################################################################
    def check_valid(self, listfullnames): 
        """ Check if a valid file of the filetype """

        results = {}
        for fname in listfullnames:
            results[fname] = True

        return results

    ######################################################################
    def ingest_contents(self, fullname, **kwargs): # e.g., Rasicam
        """ Ingest certain content into a non-metadata table """
        pass

    ######################################################################
    def perform_metadata_tasks(self, fullname, do_update, update_info):
        """ Read metadata from file, updating file values """

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: beg")

        # read metadata and call any special calc functions
        metadata = self._gather_metadata_file(fullname)

        if do_update:
            miscutils.fwdebug_print("WARN (%s): skipping file metadata update." % \
                                    self.__class__.__name__)

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: end")
        return metadata


    ######################################################################
    def _gather_metadata_file(self, fullname, **kwargs):
        """ Gather metadata for a single file """

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: beg  file=%s" % (fullname))

        metadata = OrderedDict()

        metadefs = self.config['filetype_metadata'][self.filetype]
        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: metadefs=%s" % (metadefs))
        for hdname, hddict in metadefs['hdus'].items():
            for status_sect in hddict:  # don't worry about missing here, ingest catches
                # get value from wcl/config
                if 'w' in hddict[status_sect]:
                    metakeys = hddict[status_sect]['w'].keys()
                    mdata2 = self._gather_metadata_from_config(fullname, metakeys)
                    metadata.update(mdata2)

                # get value directly from header
                if 'h' in hddict[status_sect]:
                    miscutils.fwdie("ERROR (%s): cannot read values from header = %s" % \
                                    (self.__class__.__name__, hddict[status_sect]['h'].keys()), 1)

                # calculate value from different header values(s)
                if 'c' in hddict[status_sect]:
                    miscutils.fwdie("ERROR (%s): cannot calculate values = %s" % \
                                    (self.__class__.__name__, hddict[status_sect]['c'].keys()), 1)

                # copy value from 1 hdu to primary
                if 'p' in hddict[status_sect]:
                    miscutils.fwdie("ERROR (%s): cannot copy values between headers = %s" % \
                                    (self.__class__.__name__, hddict[status_sect]['p'].keys()), 1)

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: end")
        return metadata


    ######################################################################
    def _gather_metadata_from_config(self, fullname, metakeys):
        """ Get values from config """
        metadata = OrderedDict()

        for wclkey in metakeys:
            metakey = wclkey.split('.')[-1]
            if metakey == 'fullname':
                metadata['fullname'] = fullname
            elif metakey == 'filename':
                metadata['filename'] = miscutils.parse_fullname(fullname, 
                                                                miscutils.CU_PARSE_FILENAME)
            elif metakey == 'filetype':
                metadata['filetype'] = self.filetype
            else:
                if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
                    miscutils.fwdebug_print("INFO: wclkey=%s" % (wclkey))
                metadata[metakey] = self.config[wclkey]

        return metadata

