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

class FiletypeMgmtGeneric(object):
    """  Base/generic class for managing a filetype (get metadata, update metadata, etc) """

    ######################################################################
    def __init__(self, filetype, dbh, config):
        """ Initialize object """
        # config must have filetype_metadata and file_header_info
        self.filetype = filetype
        self.dbh = dbh
        self.config = config

    ######################################################################
    def perform_metadata_tasks(self, fullname, do_update, update_info):
        """ Read metadata from file, updating file values """

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: beg")

        # read metadata and call any special calc functions
        metadata = self.gather_metadata_file(fullname)

        if do_update:
            miscutils.fwdebug_print("WARN: Generic filetype mgmt cannot update a file's metadata")

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: end")
        return metadata


    ######################################################################
    def gather_metadata_file(self, fullname, **kwargs):
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
                    mdata2 = self.gather_metadata_from_config(fullname, metakeys)
                    metadata.update(mdata2)

                # get value directly from header
                if 'h' in hddict[status_sect]:
                    miscutils.fwdie("ERROR: Generic filetype mgmt cannot get values from header = %s" % \
                                    (hddict[status_sect]['h'].keys()), 1)

                # calculate value from different header values(s)
                if 'c' in hddict[status_sect]:
                    miscutils.fwdie("ERROR: Generic filetype mgmt cannot calculate values = %s" % \
                                    (hddict[status_sect]['c'].keys()), 1)

                # copy value from 1 hdu to primary
                if 'p' in hddict[status_sect]:
                    miscutils.fwdie("ERROR: Generic filetype mgmt cannot copy values between headers = %s" % \
                                    (hddict[status_sect]['p'].keys()), 1)

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_check("INFO: end")
        return metadata


    ######################################################################
    def gather_metadata_from_config(self, fullname, metakeys):
        """ Get values from config """
        metadata = OrderedDict()

        if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: fullname=%s" % (fullname))
            miscutils.fwdebug_print("INFO: metakeys=%s" % (metakeys))

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

    ######################################################################
    def check_valid(self, fullname): # should raise exception if not valid
        """ Check if a valid file of the filetype """
        pass

    ######################################################################
    def ingest_contents(self, fullname): # e.g., Rasicam
        """ Ingest certain content into a non-metadata table """
        pass
