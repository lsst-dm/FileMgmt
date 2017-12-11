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
import despyfitsutils.fits_special_metadata as spmeta
import despyfitsutils.fitsutils as fitsutils

class FtMgmtGenFits(FtMgmtGeneric):
    """  Base/generic class for managing a filetype (get metadata, update metadata, etc) """

    ######################################################################
    def __init__(self, filetype, dbh, config, filepat=None):
        """ Initialize object """
        # config must have filetype_metadata and file_header_info
        FtMgmtGeneric.__init__(self, filetype, dbh, config, filepat)

    ######################################################################
    def perform_metadata_tasks(self, fullname, do_update, update_info):
        """ Read metadata from file, updating file values """

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: beg")

        # open file
        if do_update:
            hdulist = pyfits.open(fullname, 'update')
        else:
            hdulist = pyfits.open(fullname)

        # read metadata and call any special calc functions
        metadata, datadefs = self._gather_metadata_file(fullname, hdulist=hdulist)
        if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: file=%s" % (fullname))

        # call function to update headers
        if do_update:
            self._update_headers_file(hdulist, metadata, datadefs, update_info)

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
        datadef = OrderedDict()

        metadefs = self.config['filetype_metadata'][self.filetype]
        for hdname, hddict in metadefs['hdus'].items():
            for status_sect in hddict:  # don't worry about missing here, ingest catches
                # get value from filename
                if 'f' in hddict[status_sect]:
                    metakeys = hddict[status_sect]['f'].keys()
                    mdata2 = self._gather_metadata_from_filename(fullname, metakeys)
                    metadata.update(mdata2)

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
                    mdata2, ddef2 = self._gather_metadata_from_header(fullname, hdulist,
                                                                      hdname, metakeys)
                    metadata.update(mdata2)
                    datadef.update(ddef2)

                # calculate value from different header values(s)
                if 'c' in hddict[status_sect]:
                    for funckey in hddict[status_sect]['c'].keys():
                        try:
                            specmf = getattr(spmeta, 'func_%s' % funckey.lower())
                        except AttributeError:
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
                    mdata2, ddef2 = self._gather_metadata_from_header(fullname, hdulist,
                                                                      hdname, metakeys)
                    #print 'ddef2 = ', ddef2
                    metadata.update(mdata2)
                    datadef.update(ddef2)

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: metadata = %s" % metadata)
            miscutils.fwdebug_print("INFO: datadef = %s" % datadef)
            miscutils.fwdebug_print("INFO: end")
        return metadata, datadef


    ######################################################################
    def _get_update_values_metadata(self, metadata, datadefs):
        """ Put metadata values for update in data structure easy to use """

        metadefs = self.config['filetype_metadata'][self.filetype]
        update_info = OrderedDict()
        update_info[0] = OrderedDict()   # update primary header

        for hdname, hddict in metadefs['hdus'].items():
            update_info[hdname] = OrderedDict()
            for stdict in hddict.values():
                # include values created by metadata functions and those copied from other hdu
                for derived in ['c', 'p', 'w']:
                    if derived in stdict:
                        for key in stdict[derived]:
                            uvalue = ucomment = udatatype = None
                            # we don't write filetype nor pfw_attempt_id to headers
                            if key == 'filename':
                                # write filename to header as DESFNAME
                                fitscomment = 'DES production filename'

                                # shorten comment if file name is so long the comment won't fit
                                if len(metadata['filename']) + \
                                        len('\' / %s' % fitscomment) + \
                                        len('DESFNAME= \'') > 80:
                                    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
                                        miscutils.fwdebug_print("WARN: %s's filename too long for DESFNAME: %s" % \
                                            (metadata['filename'], len(metadata['filename'])))
                                        fitscomment = fitscomment[:min(len(fitscomment), 80 - len(metadata['filename']) - 16)]
                                     
                                update_info[0]['DESFNAME'] = (metadata['filename'], fitscomment, 'str')
                                     
                            elif key != 'filetype' and key != 'pfw_attempt_id':
                                if key in metadata:
                                    uvalue = metadata[key]
                                    if key in datadefs:
                                        ucomment = datadefs[key][0]
                                        udatatype = datadefs[key][1]
                                    elif miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
                                        miscutils.fwdebug_print("WARN: could not find comment for key=%s" % (key))
                                    update_info[0][key] = (uvalue, ucomment, udatatype)
                                else:
                                    miscutils.fwdebug_print("WARN: could not find metadata for key=%s" % (key))
        return update_info

    ######################################################################
    def _get_file_header_key_info(self, key):
        """ From definitions of file header keys, return comment and fits data type """

        file_header_info = self.config['file_header']
        ucomment = None
        udatatype = None
        if key in file_header_info:
            if 'description' in file_header_info[key]:
                ucomment = file_header_info[key]['description']
            else:
                miscutils.fwdebug_print("WARN: could not find description for key=%s" % (key))

            if 'fits_data_type' in file_header_info[key]:
                udatatype = file_header_info[key]['fits_data_type']
            else:
                miscutils.fwdebug_print("WARN: could not find fits_data_type for key=%s" % (key))
        return ucomment, udatatype


    ######################################################################
    def _get_update_values_explicit(self, update_info):
        """ include values explicitly set by operator/framework """

        upinfo2 = OrderedDict()

        # for each set of header updates
        for updset in update_info.values():
            headers = ['0']   # default to primary header
            if 'headers' in updset:
                headers = miscutils.fwsplit(update_info[updset], ',')

            hdu_updset = OrderedDict()
            for key, val in updset.items():
                if key != 'headers':
                    uval = ucomment = udatatype = None
                    header_info = miscutils.fwsplit(val, '/')
                    uval = header_info[0]
                    if len(header_info) == 3:
                        ucomment = header_info[1]
                        udatatype = header_info[2]
                    hdu_updset[key] = (uval, ucomment, udatatype)

            for hdname in headers:
                if hdname not in update_info:
                    upinfo2[hdname] = OrderedDict()

                upinfo2[hdname].update(hdu_updset)

        return upinfo2



    ######################################################################
    @classmethod
    def _gather_metadata_from_header(cls, fullname, hdulist, hdname, metakeys):
        """ Get values from config """

        metadata = OrderedDict()
        datadef = OrderedDict()
        for key in metakeys:
            if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
                miscutils.fwdebug_print("INFO: key=%s" % (key))
            try:
                metadata[key] = fitsutils.get_hdr_value(hdulist, key.upper(), hdname)
                datadef[key] = fitsutils.get_hdr_extra(hdulist, key.upper(), hdname)
            except KeyError:
                if miscutils.fwdebug_check(1, 'FTMGMT_DEBUG'):
                    miscutils.fwdebug_print("INFO: didn't find key %s in %s header of file %s" %\
                                            (key, hdname, fullname))

        return metadata, datadef

    ######################################################################
    def _update_headers_file(self, hdulist, metadata, datadefs, update_info):
        """ Update headers in file """

        #update_info = hdrupd section of input wcl, dictionary of dictionary
        #<hdrupd>
        #    <set_0>
        #        headers = 0, 1
        #        key1 = value1/descr/fits data type
        #        key2 = value2/descr/fits data type
        #    </set_0>
        #</hdrupd>

        all_update_info = self._get_update_values_metadata(metadata, datadefs)
        wcl_update_info = self._get_update_values_explicit(update_info)
        all_update_info.update(wcl_update_info)

        # update values in file
        for hdname in all_update_info:
            newhdname = hdname
            try:
                newhdname = int(hdname)
            except ValueError:
                newhdname = hdname

            hdr = hdulist[newhdname].header
            for key, info in all_update_info[hdname].items():
                uval = info[0]
                ucomment = info[1]
                udatatype = info[2]

                if ucomment is None:
                    ucomment, udatatype = self._get_file_header_key_info(key)
                elif udatatype is None:
                    _, udatatype = self._get_file_header_key_info(key)

                if type(udatatype) is str and type(uval) is str and udatatype != 'str':
                    udatatype = udatatype.lower()
                    #global __builtins__
                    #uval = getattr(__builtins__, udatatype)(uval)
                    if udatatype == 'int':
                        uval = int(uval)
                    elif udatatype == 'float':
                        uval = float(uval)
                    elif udatatype == 'bool':
                        uval = bool(uval)

                if ucomment is not None:
                    hdr[key.upper()] = (uval, ucomment)
                else:
                    hdr[key.upper()] = uval
