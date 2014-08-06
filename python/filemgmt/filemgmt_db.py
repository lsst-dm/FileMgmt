#!/usr/bin/env python

# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
    Define a database utility class extending coreutils.DesDbi

    Developed at:
    The National Center for Supercomputing Applications (NCSA).

    Copyright (C) 2012 Board of Trustees of the University of Illinois.
    All rights reserved.
"""

__version__ = "$Rev$"

import os
import sys
import socket
import coreutils
import argparse
from collections import OrderedDict

from coreutils.miscutils import *
from filemgmt.filemgmt_defs import *
from filemgmt.errors import *
import filemgmt.utils as fmutils

class FileMgmtDB (coreutils.DesDbi):
    """
        Extend coreutils.DesDbi with functionality for managing files (metadata ingestion, "location" registering)
    """

    @staticmethod
    def requested_config_vals():
        """ return dictionary describing what values this class uses along with whether they are optional or required """
        return {'use_db':'opt', 'archive':'req', FILE_HEADER_INFO:'opt', 'filetype_metadata':'req', 'des_services':'opt', 'des_db_section':'req'}

    def __init__ (self, config=None, argv=None):
        parser = argparse.ArgumentParser(description='FileMgmtDB')
        parser.add_argument('--config', action='store')
        parser.add_argument('--use_db', action='store')
        parser.add_argument('--desservices', action='store', default=None)
        parser.add_argument('--section', action='store', default=None)
        args, unknown_args = parser.parse_known_args()
        args = vars(args)   # turn into dictionary
        
        if args['use_db'] is None:
            self.use_db = use_db(config)
        else:
            self.use_db = use_db(args)

        if not self.use_db:
            fwdie("Error:  FileMgmtDB class requires DB but was told not to use DB", 1)

        self.desservices = args['desservices']
        if self.desservices is None and config is not None and 'des_services' in config:
            self.desservices = config['des_services']

        self.section = args['section']
        if self.section is None and config is not None and 'des_db_section' in config:
            self.section = config['des_db_section']
            
        try:
            coreutils.DesDbi.__init__ (self, self.desservices, self.section)
        except Exception as err:
            fwdie("Error: problem connecting to database: %s\n\tCheck desservices file and environment variables" % err, 1)


        # precedence - db, file or func arg
        self.config = OrderedDict()
        self._get_config_from_db(args)

        if config is not None:
            self.config.update(config)
        else:
            fileconfig = None
            if 'config' in args and args['config'] is not None:
                import intgutils.wclutils as wclutils
                with open(args['config'], 'r') as fh:
                    fileconfig = wclutils.read_wcl(fh)
                    self.config.update(fileconfig)


    def _get_config_from_db(self, args):
        """ reads some configuration values from the database """
        self.config = OrderedDict()
        self.config['archive']  = self.get_archive_info()
        self.config['filetype_metadata'] = self.get_all_filetype_metadata()  
        self.config[FILE_HEADER_INFO] = self.query_results_dict('select * from OPS_FILE_HEADER', 'name')



    def get_list_filenames(self, args):
        # args is an array of "command-line" args possibly with keys for query
        # returns python list of filenames

        # get metatable's columns to parse from args 
        parser = argparse.ArgumentParser(description='get_src_file_path')
        parser.add_argument('--metatable', action='store', default=None)
        myargs, unknown = parser.parse_known_args(args)
        myargs = vars(myargs)
        if myargs['metatable'] is None:
            fwdie("Error: must specify either list or metatable", 1)
        metatable = myargs['metatable']
        
        # parse all args for column keys in order to build query
        colnames = self.get_column_names(metatable);
        parser = argparse.ArgumentParser(description='get_src_file_path')
        for c in colnames:
            parser.add_argument('--%s' % c, action='store', default=None)
        myargs, unknown = parser.parse_known_args(args)
        myargs = vars(myargs)
        key_vals = { k : myargs[k] for k in myargs if myargs[k] != None }

        # create query string to get filenames
        querydict = {metatable: {'select_fields': ['filename'], 'key_vals': key_vals}}
        querystr = self.create_query_string(querydict)
        curs = self.cursor()
        curs.execute(querystr)
        filelist = []
        for line in curs:
            filelist.append(line[0])

        return filelist
            

    def register_file_in_archive(self, filelist, args):
        """ Saves filesystem information about file like relative path in archive, compression extension, size, etc """
        # assumes files have already been declared to database (i.e., metadata)
        # caller of program must have already verified given filelist matches given archive
        # if giving fullnames, must include archive root
        # keys to each file dict must be lowercase column names, missing data must be None 


        fwdebug(3, 'FILEMGMT_DEBUG', "filelist = %s" % filelist)
        archivename = args['archive']
        archivedict = self.config['archive'][archivename]
        archiveroot = archivedict['root']

        if len(filelist) != 0:
            insfilelist = []
            for f in filelist:
                filedict = {}
                filedict['archive_name'] = archivename
                if 'filename' in filelist[f] and 'path' in filelist[f] and 'compression' in filelist[f]:
                    filedict['filename'] = filelist[f]['filename']
                    filedict['compression'] = filelist[f]['compression']
                    path = filelist[f]['path']
                elif 'fullname' in filelist[f]:
                    #print "getting file parts from fullname"
                    (path, filedict['filename'], filedict['compression']) = parse_fullname(filelist[f]['fullname'], 7)  
                else:
                    fwdie("Error:   Incomplete info for a file to register.   Given %s" % filelist[f], 1)

                #print "filedict = ", filedict
                if filedict['compression'] is not None and not re.match('^\.', filedict['compression']):   # make sure compression starts with .
                    filedict['compression'] = '.' + filedict['compression']

                if re.match('^/', path):   # if path is absolute
                    if re.match('^%s/' % archiveroot, path):  # get rid of the archive root from the path to store
                        filedict['path'] = path[len(archiveroot)+1:]
                    else:
                        canon_archroot = os.path.realpath(archiveroot)
                        canon_path = os.path.realpath(path)
                        if re.match('^%s/' % canon_archroot, canon_path):  # get rid of the archive root from the path to store
                            filedict['path'] = canon_path[len(canon_archroot)+1:]
                        else:
                            fwdie("Error: file's absolute path (%s) does not contain the archive root (%s) (filedict:%s)" % (path, archiveroot, filedict), 1 )
                else:
                    filedict['path'] = path # assume only contains the relative path within the archive

                if 'filesize' in filelist[f]:
                    filedict['filesize'] = filelist[f]['filesize']
                else:
                    #filedict['filesize'] = os.path.getsize(filelist[f]['fullname'])
                    fwdie('Error: filename (%s) does not have a filesize (%s)' % (f, filedict), 1)

                insfilelist.append(filedict)

            try:
                colnames = self.get_column_names('FILE_ARCHIVE_INFO');
                self.insert_many_indiv('FILE_ARCHIVE_INFO', colnames, insfilelist)
            except:
                print "Error from insert_many_indiv in register_file_archive"
                print "colnames =", colnames
                print "filelist =", filelist
                raise
            

    def file_has_metadata(self, filenames):
        """ Check whether metadata has been ingested for given files """
        dbq = "select filename from genfile where filename in ('%s')" % "','".join(filenames)
        curs = self.cursor()
        curs.execute(dbq)
        havelist = []
        for row in curs:
            havelist.append(row[0])
        return havelist 

    def is_file_in_archive(self, fnames, filelist, args):
        """ Checks whether given files are in the specified archive according to the DB """
        # TODO change to return count(*) = 0 or 1 which would preserve array
        #      another choice is to return path, but how to make it return null for path that doesn't exist
        #      to make faster don't do concat in query do bind query with filename= and compression=

        archivename = args['archive']
        archivedict = self.config['archive'][archivename]
        
        sql = "select filename||compression from file_archive_info where archive_name = '%s' and filename||compression in ('%s')" % (archivename, "','".join(fnames))
        fwdebug(1, "FILEMGMTDB_DEBUG", "sql = %s" % sql)
        curs = self.cursor()
        curs.execute(sql)
        existslist = []
        for row in curs:
            existslist.append(row[0])
        return existslist


    def _get_required_headers(self, filetypeDict):
        """
        For use by ingest_file_metadata. Collects the list of required header values.
        """
        REQUIRED = "r"
        allReqHeaders = set()
        for hdu,hduDict in filetypeDict.iteritems():
            if type(hduDict) in (OrderedDict,dict):
                if REQUIRED in hduDict:
                    for category,catDict in hduDict[REQUIRED].iteritems():
                        allReqHeaders = allReqHeaders.union(catDict.keys())
        return allReqHeaders


    def _get_column_map(self, filetypeDict):
        """
        For use by ingest_file_metadata. Creates a lookup from column to header.
        """
        columnMap = OrderedDict()
        for hduDict in filetypeDict.values():
            if type(hduDict) in (OrderedDict,dict):
                for statusDict in hduDict.values():
                    for catDict in statusDict.values():
                        for header, columns in catDict.iteritems():
                            collist = columns.split(',')
                            for position, column in enumerate(collist):
                                if len(collist) > 1:
                                    columnMap[column] = header + ":" + str(position)
                                else:
                                    columnMap[column] = header
        return columnMap


    def ingest_file_metadata(self, filemeta):
        """
        Ingests the file metadata stored in <filemeta> into the database,
        using <dbdict> to determine where each element belongs.
        This wil throw an error and abort if any of the following are missing
        for any file: the filename, filetype, or other required header value.
        It will also throw an error if the filetype given in the input data
        is not found in <dbdict>
        Any exception will abort the entire upload.
        """
        dbdict = self.config[FILETYPE_METADATA]
        FILETYPE  = "filetype"
        FILENAME  = "filename"
        METATABLE = "metadata_table"
        COLMAP    = "column_map"
        ROWS      = "rows"
        metadataTables = OrderedDict()
        message = ""
        fullMessage = ""
        
        try:
            for key, filedata in filemeta.iteritems():
                foundError = 0
                if FILENAME not in filedata.keys():
                    fullMessage = '\n'.join(["ERROR: cannot upload file <" + key + ">, no FILENAME provided.",fullMessage])
                    continue
                if FILETYPE not in filedata.keys():
                    fullMessage = '\n'.join(["ERROR: cannot upload file " + filedata[FILENAME] + ": no FILETYPE provided.",fullMessage])
                    continue
                if filedata[FILETYPE] not in dbdict:
                    fullMessage = '\n'.join(["ERROR: cannot upload " + filedata[FILENAME] + ": " + \
                            filedata[FILETYPE] + " is not a known FILETYPE.",fullMessage])
                    continue
                # check that all required are present
                allReqHeaders = self._get_required_headers(dbdict[filedata[FILETYPE]])
                for dbkey in allReqHeaders:
                    if dbkey not in filedata.keys() or filedata[dbkey] == "":
                        #print 'fullMessage=',fullMessage, type(fullMessage)
                        fullMessage = '\n'.join(["ERROR: " + filedata[FILENAME] + " missing required data for " + dbkey,fullMessage])
                
                # any error should then skip all upload attempts
                if fullMessage:
                    continue

                # now load structures needed for upload
                rowdata = OrderedDict()
                mappedHeaders = set()
                fileMetaTable = dbdict[filedata[FILETYPE]][METATABLE]

                if fileMetaTable not in metadataTables.keys():
                    metadataTables[fileMetaTable] = OrderedDict()
                    metadataTables[fileMetaTable][COLMAP] = self._get_column_map(dbdict[filedata[FILETYPE]])
                    metadataTables[fileMetaTable][ROWS] = []
                
                colmap = metadataTables[fileMetaTable][COLMAP]
                for column, header in colmap.iteritems():
                    compheader = header.split(':')
                    if len(compheader) > 1:
                        hdr = compheader[0]
                        pos = int(compheader[1])
                        if hdr in filedata:
                            rowdata[column] = filedata[hdr].split(',')[pos]
                            mappedHeaders.add(hdr)
                    else:
                        if header in filedata:
                            rowdata[column] = filedata[header]
                            mappedHeaders.add(header)
                        else:
                            rowdata[column] = None
                
                # report elements that were in the file that do not map to a DB column
                for notmapped in (set(filedata.keys()) - mappedHeaders):
                    if notmapped != 'fullname':
                        print "WARN: file " + filedata[FILENAME] + " header item " \
                            + notmapped + " does not match column for filetype " \
                            + filedata[FILETYPE]
                
                # add the new data to the table set of rows
                metadataTables[fileMetaTable][ROWS].append(rowdata)
            # end looping through files
            
            for metaTable, dict in metadataTables.iteritems():
                #self.insert_many(metaTable, dict[COLMAP].keys(), dict[ROWS])
                self.insert_many_indiv(metaTable, dict[COLMAP].keys(), dict[ROWS])
            #self.commit()
            
        except Exception as ex:
            #raise FileMetadataIngestError(ex)
            raise

        if fullMessage:
            print >> sys.stderr, fullMessage
            raise RequiredMetadataMissingError(fullMessage)

    # end ingest_file_metadata


    def is_valid_filetype(self, ftype):
        """ Checks filetype definitions to determine if given filetype exists """
        if ftype.lower() in self.config[FILETYPE_METADATA]:
            return True
        else:
            return False

    def is_valid_archive(self, arname):
        """ Checks archive definitions to determine if given archive exists """
        if arname.lower() in self.config['archive']:
            return True
        else:
            return False


    def get_file_location(self, filelist, arname, compress_order=FM_PREFER_COMPRESSED):
        """ Return relative archive paths and filename including any compression extenstion """

        fileinfo = self.get_file_archive_info(filelist, arname, compress_order)
        rel_filenames = {}
        for f, finfo in fileinfo.items():
            rel_filenames[f] = finfo['rel_filename']
        return rel_filenames


    def get_file_archive_info(self, filelist, arname, compress_order=FM_PREFER_COMPRESSED):
        """ Return information about file stored in archive (e.g., filename, size, rel_filename, ...) """

        # sanity checks
        if 'archive' not in self.config:
            fwdie('Error: Missing archive section in config', 1)

        if arname not in self.config['archive']:
            fwdie('Error: Invalid archive name (%s)' % arname, 1)

        if 'root' not in self.config['archive'][arname]:
            fwdie('Error: Missing root in archive def (%s)' % self.config['archive'][arname], 1)

        if not isinstance(compress_order, list):
            fwdie('Error:  Invalid compress_order.  It must be a list of compression extensions (including None)')

        # query DB getting all files regardless of compression
        #     Can't just use 'in' expression because could be more than 1000 filenames in list
        #           ORA-01795: maximum number of expressions in a list is 1000
    
        # insert filenames into filename global temp table to use in join for query
        gtt_name = self.load_filename_gtt(filelist)

        # join to GTT_FILENAME for query
        result = []
        cursor = self.cursor()
        sql = "select filetype,file_archive_info.* from genfile,file_archive_info,%(gtt)s where archive_name=%(ar)s and genfile.filename=%(gtt)s.filename and file_archive_info.filename=%(gtt)s.filename" % ({'ar':self.get_named_bind_string('archive_name'), 'gtt':gtt_name})
        curs = self.cursor()
        curs.execute(sql, {'archive_name': arname})
        desc = [d[0].lower() for d in curs.description]

        fullnames = {}
        for p in compress_order:
            fullnames[p] = {}

        for line in curs:
            d = dict(zip(desc, line))
            
            #print "line = ", line
            if d['compression'] is None:
                compext = ""
            else:
                compext = d['compression']
            d['rel_filename'] = "%s/%s%s" % (d['path'], d['filename'], compext)
            fullnames[d['compression']][d['filename']] = d
        curs.close()
        
        self.empty_gtt(gtt_name)
        
        #print "uncompressed:", len(fullnames[None])
        #print "compressed:", len(fullnames['.fz'])

        # go through given list of filenames and find archive location and compreesion
        archiveinfo = {}
        for name in filelist:
            #print name
            for p in compress_order:    # follow compression preference
                #print "p = ", p
                if name in fullnames[p]:
                    archiveinfo[name] = fullnames[p][name]
                    break

        #print "archiveinfo = ", archiveinfo
        return archiveinfo


    def get_file_archive_info_path(self, path, arname, compress_order=FM_PREFER_COMPRESSED):
        """ Return information about file stored in archive (e.g., filename, size, rel_filename, ...) """

        # sanity checks
        if 'archive' not in self.config:
            fwdie('Error: Missing archive section in config', 1)

        if arname not in self.config['archive']:
            fwdie('Error: Invalid archive name (%s)' % arname, 1)

        if 'root' not in self.config['archive'][arname]:
            fwdie('Error: Missing root in archive def (%s)' % self.config['archive'][arname], 1)

        if not isinstance(compress_order, list):
            fwdie('Error:  Invalid compress_order.  It must be a list of compression extensions (including None)')

        likestr = self.get_regex_clause('path', '%s/.*' % path)

        # query DB getting all files regardless of compression
        sql = "select filetype,file_archive_info.* from genfile,file_archive_info where archive_name='%s' and genfile.filename=file_archive_info.filename and %s" % (arname, likestr)
        print "sql>", sql
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]

        fullnames = {}
        for p in compress_order:
            fullnames[p] = {}

        list_by_name = {}
        for line in curs:
            d = dict(zip(desc, line))
            
            #print "line = ", line
            if d['compression'] is None:
                compext = ""
            else:
                compext = d['compression']
            d['rel_filename'] = "%s/%s%s" % (d['path'], d['filename'], compext)
            fullnames[d['compression']][d['filename']] = d
            list_by_name[d['filename']] = True

        #print "uncompressed:", len(fullnames[None])
        #print "compressed:", len(fullnames['.fz'])

        # go through given list of filenames and find archive location and compreesion
        archiveinfo = {}
        for name in list_by_name.keys():
            #print name
            for p in compress_order:    # follow compression preference
                #print "p = ", p
                if name in fullnames[p]:
                    archiveinfo[name] = fullnames[p][name]
                    break

        #print "archiveinfo = ", archiveinfo
        return archiveinfo


    ##########

    def createArtifacts(self, filelist):
        """ Verifies that artifacts exist for all the files in filelist """
        sqlstr = """
            insert into OPM_ARTIFACT (name, compression) 
            select filename, compression
            from OPM_FILENAME_GTT gt
            where not exists(
                select * from opm_artifact a2 where a2.name=gt.filename and 
                    NVL(a2.compression,'1') = NVL(gt.compression,'1')
                )"""
        gtt_name = self.load_filename_gtt(filelist)
        cur = self.cursor()
        cur.execute(sqlstr)
        cur.close()
        self.empty_gtt(gtt_name)
    # end createArtifacts()

    def getFilenameIdMap(self, prov):
        DELIM = ","
        USED  = "used"
        WGB   = "was_generated_by"
        WDF   = "was_derived_from"

        allfiles = set()
        if USED in prov:
            for filenames in prov[USED].values():
                for file in filenames.split(DELIM):
                    allfiles.add(file.strip())
        if WGB in prov:
            for filenames in prov[WGB].values():
                for file in filenames.split(DELIM):
                    allfiles.add(file.strip())
        if WDF in prov:
            for tuples in prov[WDF].values():
                for filenames in tuples.values():
                    for file in filenames.split(DELIM):
                        allfiles.add(file.strip())

        result = []
        if len(allfiles) > 0:
            gtt_name = self.load_filename_gtt(allfiles)
            sqlstr = """SELECT f.filename || f.compression, a.ID 
                FROM OPM_ARTIFACT a, %s f 
                WHERE a.name=f.filename and (a.compression=f.compression or (
                    a.compression is null and f.compression is null))""" % (gtt_name)
            cursor = self.cursor()
            cursor.execute(sqlstr)
            result = cursor.fetchall()
            cursor.close()
            self.empty_gtt(gtt_name)

            return dict(result)
        else:
            return result
    # end getFilenameIdMap


    def ingest_provenance(self, prov, execids):
        DELIM = ","
        USED  = "used"
        WGB   = "was_generated_by"
        WDF   = "was_derived_from"
        TASK_ID = "TASK_ID"
        OPM_ARTIFACT_ID = "OPM_ARTIFACT_ID"
        PARENT_OPM_ARTIFACT_ID = "PARENT_OPM_ARTIFACT_ID"
        CHILD_OPM_ARTIFACT_ID  = "CHILD_OPM_ARTIFACT_ID"
        USED_TABLE = "OPM_USED"
        WGB_TABLE  = "OPM_WAS_GENERATED_BY"
        WDF_TABLE  = "OPM_WAS_DERIVED_FROM"
        COLMAP_USED_WGB = [TASK_ID,OPM_ARTIFACT_ID]
        COLMAP_WDF = [PARENT_OPM_ARTIFACT_ID,CHILD_OPM_ARTIFACT_ID]
        PARENTS = "parents"
        CHILDREN = "children"

        insertSQL = """insert into %s d (%s) select %s,%s %s where not exists(
                    select * from %s n where n.%s=%s and n.%s=%s)"""

        data = []
        bindStr = self.get_positional_bind_string()
        cursor = self.cursor()
        filemap = self.getFilenameIdMap(prov)

        if USED in prov:
            for execname, filenames in prov[USED].iteritems():
                for file in filenames.split(DELIM):
                    rowdata = []
                    rowdata.append(execids[execname])
                    rowdata.append(filemap[file.strip()])
                    rowdata.append(execids[execname])
                    rowdata.append(filemap[file.strip()])
                    data.append(rowdata)
            execSQL = insertSQL % (USED_TABLE, TASK_ID + "," + OPM_ARTIFACT_ID,
                bindStr, bindStr,self.from_dual(), USED_TABLE, TASK_ID, bindStr,
                OPM_ARTIFACT_ID, bindStr)
            cursor.executemany(execSQL, data)
            data = []

        if WGB in prov:
            for execname, filenames in prov[WGB].iteritems():
                for file in filenames.split(DELIM):
                    rowdata = []
                    rowdata.append(execids[execname])
                    rowdata.append(filemap[file.strip()])
                    rowdata.append(execids[execname])
                    rowdata.append(filemap[file.strip()])
                    data.append(rowdata)
            execSQL = insertSQL % (WGB_TABLE, TASK_ID + "," + OPM_ARTIFACT_ID,
                bindStr, bindStr, self.from_dual(), WGB_TABLE, TASK_ID, bindStr,
                OPM_ARTIFACT_ID, bindStr)
            cursor.executemany(execSQL, data)
            data = []

        if WDF in prov:
            for tuples in prov[WDF].values():
                for parentfile in tuples[PARENTS].split(DELIM):
                    for childfile in tuples[CHILDREN].split(DELIM):
                        rowdata = []
                        rowdata.append(filemap[parentfile.strip()])
                        rowdata.append(filemap[childfile.strip()])
                        rowdata.append(filemap[parentfile.strip()])
                        rowdata.append(filemap[childfile.strip()])
                        data.append(rowdata)
            execSQL = insertSQL % (WDF_TABLE, PARENT_OPM_ARTIFACT_ID + "," +
                CHILD_OPM_ARTIFACT_ID, bindStr, bindStr, self.from_dual(),
                WDF_TABLE, PARENT_OPM_ARTIFACT_ID, bindStr, CHILD_OPM_ARTIFACT_ID,
                bindStr)
            cursor.executemany(execSQL, data)
            self.commit()
    #end_ingest_provenance


