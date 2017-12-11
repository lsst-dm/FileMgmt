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

from filemgmt.ftmgmt_generic import FtMgmtGeneric

import despymisc.miscutils as miscutils
import databaseapps.datafile_ingest_utils as dfiutils

class FtMgmtDatafile(FtMgmtGeneric):
    """  Class for managing a filetype whose contents can be read by datafile_ingest """

    ######################################################################
    def __init__(self, filetype, dbh, config, filepat=None):
        """ Initialize object """
        # config must have filetype_metadata and file_header_info
        FtMgmtGeneric.__init__(self, filetype, dbh, config, filepat=None)

        [self.tablename, self.didatadefs] = self.dbh.get_datafile_metadata(filetype)

    ######################################################################
    def has_contents_ingested(self, listfullnames):
        """ Check if file has contents ingested """

        assert(isinstance(listfullnames, list))

        results = {}
        for fname in listfullnames:
            filename = miscutils.parse_fullname(fname, miscutils.CU_PARSE_FILENAME)
            results[fname] = dfiutils.is_ingested(filename, self.tablename, self.dbh) 

        return results

    ######################################################################
    def ingest_contents(self, listfullnames, **kwargs):
        """ Ingest certain content into a non-metadata table """

        assert(isinstance(listfullnames, list))

        for fname in listfullnames:
            miscutils.fwdebug_print("********************* %s" % fname)
            numrows = dfiutils.datafile_ingest_main(self.dbh, self.filetype, fname,
                                                    self.tablename, self.didatadefs)
            if numrows == None or numrows == 0:
                miscutils.fwdebug_print("WARN: 0 rows ingested from %s" % fname)
            elif miscutils.fwdebug_check(1, 'FTMGMT_DEBUG'):
                miscutils.fwdebug_print("INFO: %s rows ingested from %s" % (numrows, fname))
