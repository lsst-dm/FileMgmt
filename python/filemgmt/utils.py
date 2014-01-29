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
