#!/usr/bin/env python

# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
    Define a database utility class for tracking transfer statistics
"""

__version__ = "$Rev$"

import os
import sys
import argparse
from collections import OrderedDict

import despymisc.miscutils as miscutils
import despydmdb.desdmdbi as desdmdbi


class TransferStatsDB (desdmdbi.DesDmDbi):
    """
        Class with functionality for tracking transfer statistics in DB
    """

    @staticmethod
    def requested_config_vals():
        """ return dictionary describing what values this class uses along with whether they are optional or required """
        return {'use_db':'opt', 'des_services':'opt', 'des_db_section':'req'}


    def __init__ (self, parent_task_id, root_task_id, config):
        if not miscutils.use_db(config):
            miscutils.fwdie("Error:  TransferStatsDB class requires DB but was told not to use DB", 1)
        self.desservices = None
        if config is not None and 'des_services' in config:
            self.desservices = config['des_services']

        self.section = None
        if config is not None and 'des_db_section' in config:
            self.section = config['des_db_section']
            
        try:
            desdmdbi.DesDmDbi.__init__ (self, self.desservices, self.section)
        except Exception as err:
            miscutils.fwdie("Error: problem connecting to database: %s\n\tCheck desservices file and environment variables" % err, 1)

        self.parent_task_id = parent_task_id
        self.root_task_id = root_task_id

        if 'transfer_stats_per_file' in config:
            self.transfer_stats_per_file = miscutils.convertBool(config['transfer_stats_per_file'])
        else:
            self.transfer_stats_per_file = False

        self.__initialize_values__()


    def __initialize_values__ (self):
        self.batch_task_id = 0
        self.file_task_id = 0
        self.totbytes = 0
        self.numfiles = 0
        self.transfer_name = None
        self.src = None
        self.dst = None

    def __str__(self):
        mydict = {'batch_task_id': self.batch_task_id, 
                  'transfer_name': self.transfer_name,
                  'src': self.src,
                  'dst': self.dst,
                  'totbytes': self.totbytes,
                  'numfiles': self.numfiles,
                  'file_task_id': self.file_task_id}
            
        return str(mydict)


    def stat_beg_batch(self, transfer_name, src, dst, transclass = None):
        """ Starting a batch transfer between src and dst (archive or job scratch) """

        miscutils.fwdebug(3, 'TRANSFERSTATS_DEBUG', "beg %s %s %s %s" % (transfer_name, src, dst, transclass))
        self.transfer_name = transfer_name
        self.src = src
        self.dst = dst
        self.batch_task_id = self.create_task(name=transfer_name, 
                                              info_table='transfer_batch',
                                              parent_task_id = self.parent_task_id,
                                              root_task_id = self.root_task_id,
                                              label = None,
                                              do_begin = True,
                                              do_commit = False)

        row = {'src': src, 'dst': dst, 'transfer_class': transclass,
               'parent_task_id': self.parent_task_id, 'task_id': self.batch_task_id}

        self.basic_insert_row('transfer_batch', row)
        self.commit()

        miscutils.fwdebug(3, 'TRANSFERSTATS_DEBUG', "end")
        return self.batch_task_id


    def stat_end_batch(self, status, totbytes = 0, numfiles = 0, task_id = None):
        """ Update rows for end of a batch transfer and commit """

        miscutils.fwdebug(3, 'TRANSFERSTATS_DEBUG', "beg - %s %s %s" % (status, totbytes, task_id))
        if task_id is None:
            task_id = self.batch_task_id
        miscutils.fwdebug(3, 'TRANSFERSTATS_DEBUG', 'before end_task, info: %s' % self)
        self.end_task(task_id, status)
        wherevals = {'task_id': task_id}

        if totbytes == 0:
            totbytes = self.totbytes
        if numfiles == 0:
            numfiles = self.numfiles
        updatevals= {'total_num_bytes': totbytes, 'total_num_files': numfiles}

        self.basic_update_row('transfer_batch', updatevals, wherevals)
        self.commit()
        self.__initialize_values__()
        miscutils.fwdebug(3, 'TRANSFERSTATS_DEBUG', "end")

    def stat_beg_file(self, filename):
        """ Insert a row into a file transfer stats table (and task table) and commit """
        
        self.numfiles += 1
        self.file_task_id = -1

        if self.transfer_stats_per_file:
            miscutils.fwdebug(3, 'TRANSFERSTATS_DEBUG', "beg - %s" % filename)
            if self.batch_task_id is None:
                raise Exception('Cannot call this function without prior calling stat_beg_batch')

            row = {'filename': filename}
            row['task_id'] = self.create_task(name = 'transfer_file', 
                                          info_table = 'transfer_file',
                                          parent_task_id = self.batch_task_id,
                                          root_task_id = self.root_task_id,
                                          label = None,
                                          do_begin = True,
                                          do_commit = False)

            row['batch_task_id'] = self.batch_task_id
            self.basic_insert_row('transfer_file', row)
            self.commit()

            self.file_task_id = row['task_id']
            miscutils.fwdebug(3, 'TRANSFERSTATS_DEBUG', "end - file_task_id = %s" % self.file_task_id)
        return self.file_task_id

        

    def stat_end_file(self, status, bytes=0, task_id=None):
        """ Update rows for end of file transfer and commit """

        self.totbytes += bytes

        if self.transfer_stats_per_file:
            miscutils.fwdebug(3, 'TRANSFERSTATS_DEBUG', "beg - %s %s %s" % (status, bytes, task_id))

            if task_id is None:
                task_id = self.file_task_id
            self.end_task(task_id, status)
            wherevals = {'task_id': task_id}
            updatevals= {'bytes': bytes}

            self.basic_update_row('transfer_file', updatevals, wherevals)
            self.commit()
            miscutils.fwdebug(3, 'TRANSFERSTATS_DEBUG', "end")


