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
import pyfits

from filemgmt.ftmgmt_generic import FtMgmtGeneric
import despymisc.miscutils as miscutils
import despyfits.fits_special_metadata as spmeta
import despyfits.fitsutils as fitsutils

class FtMgmtGenFits(FtMgmtGeneric):
    """  Base/generic class for managing a filetype (get metadata, update metadata, etc) """

    ######################################################################
    def __init__(self, filetype, dbh, config):
        """ Initialize object """
        # config must have filetype_metadata and file_header_info
        FtMgmtGeneric.__init__(self, filetype, dbh, config)

    ######################################################################
    def perform_metadata_tasks(self, fullname, do_update, update_info):
        """ Read metadata from file, updating file values """

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: beg")

        # open file
        hdulist = pyfits.open(fullname, 'update')

        # read metadata and call any special calc functions
        metadata = self._gather_metadata_file(fullname, hdulist=hdulist)
        if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: file=%s" % (fullname))

        # call function to update headers
        if do_update:
            self._update_headers_file(hdulist, metadata, update_info)

        # close file
        hdulist.close()

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: end")
        return metadata

    ######################################################################
    def _gather_metadata_file(self, fullname, **kwargs):
        """ Gather metadata for a single file """

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: file=%s" % (fullname))

        hdulist = kwargs['hdulist']

        metadata = OrderedDict()

        metadefs = self.config['filetype_metadata'][self.filetype]
        for hdname, hddict in metadefs['hdus'].items():
            for status_sect in hddict:  # don't worry about missing here, ingest catches
                # get value from wcl/config
                if 'w' in hddict[status_sect]:
                    metakeys = hddict[status_sect]['w'].keys()
                    mdata2 = self._gather_metadata_from_config(fullname, metakeys)
                    metadata.update(mdata2)

                # get value directly from header
                if 'h' in hddict[status_sect]:
                    if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
                        miscutils.fwdebug_print("INFO: headers=%s" % \
                                                (hddict[status_sect]['h'].keys()))
                    metakeys = hddict[status_sect]['h'].keys()
                    mdata2 = self._gather_metadata_from_header(fullname, hdulist, hdname, metakeys)
                    metadata.update(mdata2)

                # calculate value from different header values(s)
                if 'c' in hddict[status_sect]:
                    for funckey in hddict[status_sect]['c'].keys():
                        try:
                            specmf = getattr(spmeta, 'func_%s' % funckey.lower())
                        except AttributeError as err:
                            miscutils.fwdebug_print("WARN: Couldn't find func_%s in despyfits.fits_special_metadata" % (funckey))


                        try:
                            val = specmf(fullname, hdulist, hdname)
                            metadata[funckey] = val
                        except KeyError:
                            if miscutils.fwdebug_check(1, 'FTMGMT_DEBUG'):
                                miscutils.fwdebug_print("INFO: couldn't create value for key %s in %s header of file %s" % (funckey, hdname, fullname))

                # copy value from 1 hdu to primary
                if 'p' in hddict[status_sect]:
                    metakeys = hddict[status_sect]['p'].keys()
                    mdata2 = self._gather_metadata_from_header(fullname, hdulist, hdname, metakeys)
                    metadata.update(mdata2)

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: end")
        return metadata


    ######################################################################
    def _get_update_values_metadata(self, metadata):
        """ Put metadata values for update in data structure easy to use """

        metadefs = self.config['filetype_metadata'][self.filetype]

        update_info = OrderedDict()
        for hdname, hddict in metadefs['hdus'].items():
            update_info[hdname] = OrderedDict()
            for stdict in hddict.values():
                # include values created by metadata functions and those copied from other hdu
                for derived in ['c', 'p']:
                    if derived in stdict:
                        print "stdict[derived] = ", stdict[derived]
                        for key in stdict[derived]:
                            if key in metadata:
                                (uvalue, ucomment) = self._get_value_comment(key, metadata[key])
                                update_info[hdname][key] = (uvalue, ucomment)
        return update_info


    ######################################################################
    def _get_value_comment(self, key, updset_str):
        """ convert string into value of correct data type and comment """
        # key = value/comment/fits data type
        header_info = miscutils.fwsplit(updset_str, '/')
        uval = ucomment = udatatype = None
        if len(header_info) == 3:
            (uval, ucomment, udatatype) = header_info[0:2]
        elif len(header_info) == 1:
            uval = header_info[0]
            file_header_info = self.config['file_header']
            if key in file_header_info:
                ucomment = file_header_info[key]['description']
                udatatype = file_header_info[key]['fits_data_type']

        if udatatype is not None:
            udatatype = udatatype.lower()
        if udatatype == 'int':
            uval = int(uval)
        elif udatatype == 'float':
            uval = float(uval)
        elif udatatype == 'bool':
            uval = miscutils.convertBool(uval)

        return uval, ucomment

    ######################################################################
    def _get_update_values_explicit(self, update_info):
        """ include values explicitly set by operator/framework """

        update_info = OrderedDict()

        # for each set of header updates
        for updset in update_info.values():
            headers = ['0']   # default to primary header
            if 'headers' in updset:
                headers = miscutils.fwsplit(update_info[updset], ',')

            hdu_updset = OrderedDict()
            for key, val in updset.items():
                if key != 'headers':
                    (uval, ucomment) = self._get_value_comment(key, val)
                    hdu_updset[key] = (uval, ucomment)

            for hdname in headers:
                if hdname not in update_info:
                    update_info[hdname] = OrderedDict()

                update_info[hdname].update(hdu_updset)

        return update_info



    ######################################################################
    @classmethod
    def _gather_metadata_from_header(cls, fullname, hdulist, hdname, metakeys):
        """ Get values from config """

        metadata = OrderedDict()
        for key in metakeys:
            if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
                miscutils.fwdebug_print("INFO: key=%s" % (key))
            try:
                metadata[key] = fitsutils.get_hdr_value(hdulist, key.upper(), hdname)
            except KeyError:
                if miscutils.fwdebug_check(1, 'FTMGMT_DEBUG'):
                    miscutils.fwdebug_print("INFO: didn't find key %s in %s header of file %s" %\
                                            (key, hdname, fullname))
        return metadata

    ######################################################################
    def _update_headers_file(self, hdulist, metadata, update_info):
        """ Update headers in file """

        #update_info = hdrupd section of input wcl, dictionary of dictionary
        #<hdrupd>
        #    <set_0>
        #        headers = 0, 1
        #        key1 = value1/descr/fits data type
        #        key2 = value2/descr/fits data type
        #    </set_0>
        #</hdrupd>

        all_update_info = self._get_update_values_metadata(metadata)
        all_update_info.update(self._get_update_values_explicit(update_info))

        # update values in file
        for hdname in all_update_info:
            hdr = hdulist[hdname].header
            for key, info in all_update_info[hdname].items():
                hdr[key.upper()] = (info[0], info[1])

