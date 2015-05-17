#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
Specialized functions for computing metadata
"""

import calendar
import re

import despyfits.fitsutils as fitsutils
import intgutils.intgdefs as intgdefs


######################################################################
# !!!! Function name must be all lowercase
# !!!! Function name must be of pattern func_<header key>
######################################################################


######################################################################
def func_band(filename, hdulist=None, whichhdu=None):
    """ Create band from the filter keyword """

    if hdulist is None:
        hdulist = fits_open(filename, 'readonly')

    filter = fitsutils.get_hdr_value(hdulist, 'FILTER') 
    band = filter.split(' ')[0]

    #if band not in ['u','g','r','i','z','Y','VR']:
    if band not in intgdefs.VALID_BANDS:
        raise KeyError("filter yields invalid band")

    return band


######################################################################
def func_camsym(filename, hdulist=None, whichhdu=None):
    """ Create camsys from the INSTRUME keyword """
    if hdulist is None:
        hdulist = fits_open(filnam,'readonly')

    instrume = fitsutils.get_hdr_value(hdulist, 'INSTRUME')

    return(instrume[0])

######################################################################
def func_nite(filename, hdulist=None, whichhdu=None):
    """ Create nite from the DATE-OBS keyword """

    if hdulist is None:
        hdulist = fits_open(filename, 'readonly')

    # date_obs = 'YYYY-MM-DDTHH:MM:SS.S'
    date_obs = fitsutils.get_hdr_value(hdulist, 'DATE-OBS')
    v = date_obs.split(':')
    hh = int(v[0].split('-')[2][-2:])
    if hh > 14:
        nite = v[0][:-3].replace('-','')
    else:
        y = int(v[0][0:4])
        m = int(v[0][5:7])
        d = int(v[0][8:10])-1
        if d==0:
            m = m - 1
            if m==0:
                m = 12
                y = y - 1
            d = calendar.monthrange(y,m)[1]
        nite = str(y).zfill(4)+str(m).zfill(2)+str(d).zfill(2)

    return nite


######################################################################
def func_objects(filename, hdulist=None, whichhdu=None):
    """ return the number of objects in fits catalog """
    if hdulist is None:
        hdulist = fits_open(filename, 'readonly')

    objects = fitsutils.get_hdr_value(hdulist, 'NAXIS2', 'LDAC_OBJECTS')

    return(objects)

######################################################################
def func_field(filename, hdulist=None, whichhdu=None):
    """ return the field from OBJECT fits header value """
    if hdulist is None:
        hdulist = fits_open(filename, 'readonly')

    try:
        object = fitsutils.get_hdr_value(hdulist, 'OBJECT')
    except:
        object = fitsutils.get_hdr_value(hdulist, 'OBJECT', 'LDAC_IMHEAD')
    fits_close(hdulist)

    m = re.search(" hex (\S+)", object)
    if m:
        field = m.group(1)
    else:
        raise KeyError("Cannot parse OBJECT (%s) for 'field' value in file %s" % (object, filnam))

    return(field)
