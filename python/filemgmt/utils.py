import despymisc.miscutils as miscutils


def get_config_vals(archive_info, config, keylist):
    """Search given dicts for specific values.
    """
    info = {}
    for k, stat in list(keylist.items()):
        if archive_info is not None and k in archive_info:
            info[k] = archive_info[k]
        elif config is not None and k in config:
            info[k] = config[k]
        elif stat.lower() == 'req':
            miscutils.fwdebug(0, 'FMUTILS_DEBUG', '******************************')
            miscutils.fwdebug(0, 'FMUTILS_DEBUG', 'keylist = %s' % keylist)
            miscutils.fwdebug(0, 'FMUTILS_DEBUG', 'archive_info = %s' % archive_info)
            miscutils.fwdebug(0, 'FMUTILS_DEBUG', 'config = %s' % config)
            miscutils.fwdie('Error: Could not find required key (%s)' % k, 1, 2)
    return info
