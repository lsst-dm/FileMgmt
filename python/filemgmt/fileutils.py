#!/usr/bin/env python

# $Id: desdbi.py 10292 2013-01-07 17:54:32Z mgower $
# $Rev:: 10292                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2013-01-07 11:54:32 #$:  # Date of last commit.

"""
"""

__version__ = "$Rev: 10292 $"

import re
import sys
import copy
import coreutils.desdbi
from collections import OrderedDict

# importing of DB specific modules done down inside code

def get_queryresult_dictionay( sql,curs,colnames=None):
    """
    inputs to the function are the query and a database cursor
    the function would return the result set
    """
    curs.execute(sql)
    desc = [d[0].lower() for d in curs.description]
    ret = []
    rowDict = {} 

    for line in curs:
        d = dict(zip(desc, line))
        if colnames:
            for colname in colnames:
                rowDict[colname.lower()] = d[colname.lower()]
                ret.append(rowDict)
        else:
            ret.append(d)

    return ret