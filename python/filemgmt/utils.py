from collections import OrderedDict
from coreutils.miscutils import *
from filemgmt.filemgmt_defs import *


##################################################################################################
def get_config_vals(archive_info, config, keylist):
    """ Search given dicts for specific values """
    info = {}
    for k, stat in keylist.items():
        if archive_info is not None and k in archive_info:
            info[k] = archive_info[k]
        elif config is not None and k in config:
            info[k] = config[k]
        elif stat.lower() == 'req':
            fwdebug(0, 'FMUTILS_DEBUG', '******************************')
            fwdebug(0, 'FMUTILS_DEBUG', 'keylist = %s' % keylist)
            fwdebug(0, 'FMUTILS_DEBUG', 'archive_info = %s' % archive_info)
            fwdebug(0, 'FMUTILS_DEBUG', 'config = %s' % config)
            fwdie('Error: Could not find required key (%s)' % k, 1, 2)
    return info



##################################################################################################
def create_file_metadata_dict(filetype, filetype_metadata, sectlabel = None, file_header_info=None):
    reqmeta = None
    optmeta = None
    updatemeta = None

    if filetype in filetype_metadata:
        # required
        if META_REQUIRED in filetype_metadata[filetype]:
            (reqmeta, updatemeta) = create_one_sect_metadata_info(META_REQUIRED, 
                                                                  filetype_metadata[filetype][META_REQUIRED],
                                                                  sectlabel, file_header_info) 

        # optional
        if META_OPTIONAL in filetype_metadata[filetype]:
            (optmeta, tmp_updatemeta) = create_one_sect_metadata_info(META_OPTIONAL,
                                                                  filetype_metadata[filetype][META_OPTIONAL],
                                                                  sectlabel, file_header_info)
            if tmp_updatemeta is not None:
                if updatemeta is None:
                    updatemeta = tmp_updatemeta
                else:
                    updatemeta.update(tmp_updatemeta)

    return (reqmeta, optmeta, updatemeta)


#####################################################################################################
def create_one_sect_metadata_info(derived_from, filetype_metadata, sectlabel = None, file_header_info=None):

    metainfo = OrderedDict()
    updatemeta = None

    #print "create_one_sect_metadata_info:"
    #wclutils.write_wcl(filetype_metadata)
    #wclutils.write_wcl(file_header_info)

    if META_HEADERS in filetype_metadata:
        metainfo[META_HEADERS] = ','.join(filetype_metadata[META_HEADERS].keys())

    if META_COMPUTE in filetype_metadata:
        if file_header_info is not None:   # if supposed to update headers and update DB
            updatemeta = create_update_items(derived_from, filetype_metadata[META_COMPUTE].keys(), file_header_info)
            if META_HEADERS not in metainfo:
                metainfo[META_HEADERS] = ""
            else:
                metainfo[META_HEADERS] += ','

            metainfo[META_HEADERS] += ','.join(filetype_metadata[META_COMPUTE].keys())
        else:  # just compute values for DB
            metainfo[META_COMPUTE] = ','.join(filetype_metadata[META_COMPUTE].keys())

    if META_WCL in filetype_metadata:
        wclkeys = []
        for k in filetype_metadata[META_WCL].keys():
             if sectlabel is not None:
                 wclkey = '%s.%s' % (sectlabel, k)
             else:
                 wclkey = k
             wclkeys.append(wclkey)
        metainfo[META_WCL] = ','.join(wclkeys)

    #print "create_one_sect_metadata_info:"
    #print "\tmetainfo = ", metainfo
    #print "\tupdatemeta = ", updatemeta
    return (metainfo, updatemeta)



###########################################################################
def create_update_items(metastatus, file_header_names, file_header_info, header_value=None):
    """ Create the update wcl for headers that should be updated """
    updateDict = OrderedDict()
    for name in file_header_names:
        if name not in file_header_info:
            fwdie('Error: Missing entry in file_header_info for %s' % name, FW_EXIT_FAILURE)

        # Example: $HDRFNC{BAND}/Filter identifier/str
        if header_value is not None and name in header_value:
            updateDict[name] = header_value[name]
        elif metastatus == META_REQUIRED:
            updateDict[name] = "$HDRFNC{%s}" % (name.upper())
        elif metastatus == META_OPTIONAL:
            updateDict[name] = "$OPTFNC{%s}" % (name.upper())
        else:
            fwdie('Error:  Unknown metadata metastatus (%s)' % (metastatus), FM_EXIT_FAILURE)

        if file_header_info[name]['fits_data_type'].lower() == 'none':
            fwdie('Error:  Missing fits_data_type for file header %s\nCheck entry in OPS_FILE_HEADER table' % name, FM_EXIT_FAILURE)

        # Requires 'none' to not be a valid description
        if file_header_info[name]['description'].lower() == 'none':
            fwdie('Error:  Missing description for file header %s\nCheck entry in OPS_FILE_HEADER table' % name, FM_EXIT_FAILURE)

        updateDict[name] += "/%s/%s" % (file_header_info[name]['description'],
                                        file_header_info[name]['fits_data_type'])

    return updateDict

