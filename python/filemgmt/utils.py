# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

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



#M ##################################################################################################
#M def create_file_metadata_dict(filetype, filetype_metadata, sectlabel = None, file_header_info=None):
#M     reqmeta = None
#M     optmeta = None
#M     updatemeta = None
#M 
#M     if filetype in filetype_metadata:
#M         # required
#M         if META_REQUIRED in filetype_metadata[filetype]:
#M             (reqmeta, updatemeta) = create_one_sect_metadata_info(META_REQUIRED, 
#M                                                                   filetype_metadata[filetype][META_REQUIRED],
#M                                                                   sectlabel, file_header_info) 
#M 
#M         # optional
#M         if META_OPTIONAL in filetype_metadata[filetype]:
#M             (optmeta, tmp_updatemeta) = create_one_sect_metadata_info(META_OPTIONAL,
#M                                                                   filetype_metadata[filetype][META_OPTIONAL],
#M                                                                   sectlabel, file_header_info)
#M             if tmp_updatemeta is not None:
#M                 if updatemeta is None:
#M                     updatemeta = tmp_updatemeta
#M                 else:
#M                     updatemeta.update(tmp_updatemeta)
#M 
#M     return (reqmeta, optmeta, updatemeta)
#M 
#M 
#M #####################################################################################################
#M def create_one_sect_metadata_info(derived_from, filetype_metadata, sectlabel = None, file_header_info=None):
#M 
#M     metainfo = OrderedDict()
#M     updatemeta = None
#M 
#M     #print "create_one_sect_metadata_info:"
#M     #wclutils.write_wcl(filetype_metadata)
#M     #wclutils.write_wcl(file_header_info)
#M 
#M     if META_HEADERS in filetype_metadata:
#M         metainfo[META_HEADERS] = ','.join(filetype_metadata[META_HEADERS].keys())
#M 
#M     if META_COMPUTE in filetype_metadata:
#M         if file_header_info is not None:   # if supposed to update headers and update DB
#M             updatemeta = create_update_items(derived_from, filetype_metadata[META_COMPUTE].keys(), file_header_info)
#M             if META_HEADERS not in metainfo:
#M                 metainfo[META_HEADERS] = ""
#M             else:
#M                 metainfo[META_HEADERS] += ','
#M 
#M             metainfo[META_HEADERS] += ','.join(filetype_metadata[META_COMPUTE].keys())
#M         else:  # just compute values for DB
#M             metainfo[META_COMPUTE] = ','.join(filetype_metadata[META_COMPUTE].keys())
#M 
#M     if META_COPY in filetype_metadata:
#M         if file_header_info is not None:   # if supposed to update headers and update DB
#M             updatemeta = create_copy_items(derived_from, filetype_metadata[META_COPY].keys())
#M             if META_HEADERS not in metainfo:
#M                 metainfo[META_HEADERS] = ""
#M             else:
#M                 metainfo[META_HEADERS] += ','
#M 
#M             metainfo[META_HEADERS] += ','.join(filetype_metadata[META_COMPUTE].keys())
#M         else:  # just compute values for DB
#M             metainfo[META_COMPUTE] = ','.join(filetype_metadata[META_COMPUTE].keys())
#M     if META_WCL in filetype_metadata:
#M         wclkeys = []
#M         for k in filetype_metadata[META_WCL].keys():
#M              if sectlabel is not None:
#M                  wclkey = '%s.%s' % (sectlabel, k)
#M              else:
#M                  wclkey = k
#M              wclkeys.append(wclkey)
#M         metainfo[META_WCL] = ','.join(wclkeys)
#M 
#M     #print "create_one_sect_metadata_info:"
#M     #print "\tmetainfo = ", metainfo
#M     #print "\tupdatemeta = ", updatemeta
#M     return (metainfo, updatemeta)
#M 
#M 
#M 
#M #######################################################################
#M def create_copy_items(metastatus, file_header_names):
#M     """ Create the update wcl for headers that should be copied from another header """
#M 
#M     updateDict = OrderedDict()
#M     for name in file_header_names:
#M         if metastatus == META_REQUIRED:
#M             updateDict[name] = "$REQCOPY{%s:LDAC_IMHEAD}" % (name.upper())
#M         elif metastatus == META_OPTIONAL:
#M             updateDict[name] = "$OPTCOPY{%s:LDAC_IMHEAD}" % (name.upper())
#M         else:
#M             fwdie('Error:  Unknown metadata metastatus (%s)' % (metastatus), PF_EXIT_FAILURE)
#M 
#M     return updateDict
#M 
#M 
#M 
#M ###########################################################################
#M def create_update_items(metastatus, file_header_names, file_header_info, header_value=None):
#M     """ Create the update wcl for headers that should be updated """
#M     updateDict = OrderedDict()
#M     print metastatus
#M     print file_header_names
#M 
#M     for name in file_header_names:
#M         if name not in file_header_info:
#M             fwdie('Error: Missing entry in file_header_info for %s' % name, FM_EXIT_FAILURE)
#M 
#M         # Example: $HDRFNC{BAND}/Filter identifier/str
#M         if header_value is not None and name in header_value:
#M             updateDict[name] = header_value[name]
#M         elif metastatus == META_REQUIRED:
#M             updateDict[name] = "$HDRFNC{%s}" % (name.upper())
#M         elif metastatus == META_OPTIONAL:
#M             updateDict[name] = "$OPTFNC{%s}" % (name.upper())
#M         else:
#M             fwdie('Error:  Unknown metadata metastatus (%s)' % (metastatus), FM_EXIT_FAILURE)
#M 
#M         if file_header_info[name]['fits_data_type'].lower() == 'none':
#M             fwdie('Error:  Missing fits_data_type for file header %s\nCheck entry in OPS_FILE_HEADER table' % name, FM_EXIT_FAILURE)
#M 
#M         # Requires 'none' to not be a valid description
#M         if file_header_info[name]['description'].lower() == 'none':
#M             fwdie('Error:  Missing description for file header %s\nCheck entry in OPS_FILE_HEADER table' % name, FM_EXIT_FAILURE)
#M 
#M         updateDict[name] += "/%s/%s" % (file_header_info[name]['description'],
#M                                         file_header_info[name]['fits_data_type'])
#M 
#M     return updateDict
#M 
