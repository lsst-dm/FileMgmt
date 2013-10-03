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
        Extend coreutils.DesDbi to add database access methods

        Add methods to retrieve the metadata headers required for one or more
        filetypes and to ingest metadata associated with those headers.
    """

    @staticmethod
    def requested_config_vals():
        return {'use_db':'opt', 'archive':'req', FILE_HEADER_INFO:'opt', 'filetype_metadata':'req'}

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
        if self.desservices is not None and config is not None and 'des_services' in config:
            self.desservices = config['des_services']

        self.section = args['section']
        if self.section is not None and config is not None and 'des_db_section' in config:
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


    def _get_all_filetype_metadata(self):
        """
        Gets a dictionary of dictionaries or string=value pairs representing
        data from the OPS_METADATA, OPS_FILETYPE, and OPS_FILETYPE_METADATA tables.
        This is intended to provide a complete set of filetype metadata required
        during a run.
        Note that the returned dictionary is nested based on the order of the
        columns in the select clause.  Values in columns contained in the
        "collections" list will be turned into dictionaries keyed by the value,
        while the remaining columns will become "column_name=value" elements
        of the parent dictionary.  Thus the sql query and collections list can be
        altered without changing the rest of the code.
        """
        sql = """select f.filetype,f.metadata_table,fm.status,m.derived,
                    fm.file_header_name,m.position,m.column_name
                from OPS_METADATA m, OPS_FILETYPE f, OPS_FILETYPE_METADATA fm
                where m.file_header_name=fm.file_header_name
                    and f.filetype=fm.filetype
                order by 1,2,3,4,5,6 """
        collections = ['filetype','status','derived']
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]
        result = OrderedDict()

        for row in curs:
            ptr = result
            for col, value in enumerate(row):
                normvalue = str(value).lower()
                if col >= (len(row)-3):
                    if normvalue not in ptr:
                        ptr[normvalue] = str(row[col+2]).lower()
                    else:
                        ptr[normvalue] += "," + str(row[col+2]).lower()
                    break
                if normvalue not in ptr:
                    if desc[col] in collections:
                        ptr[normvalue] = OrderedDict()
                    else:
                        ptr[desc[col]] = normvalue
                if desc[col] in collections:
                    ptr = ptr[normvalue]
        curs.close()
        self.config[FILETYPE_METADATA]=result

    def _get_config_from_db(self, args):
        self.config = OrderedDict()
        self.config['archive']  = self.get_archive_info()
        self.config['filetype_metadata'] = self.get_all_filetype_metadata()  
        self.config[FILE_HEADER_INFO] = self.query_results_dict('select * from OPS_FILE_HEADER', 'name')


    def get_metadata_specs(self, ftype, sectlabel=None, updatefits=False):
        """ Return dict/wcl describing metadata to gather for given filetype """

        #print "ftype =", ftype
        metaspecs = OrderedDict()

        # note:  When manually ingesting files generated externally to the framework, we do not want to modify the files (i.e., no updating/inserting headers
        
        file_header_info = None
        if updatefits:
            file_header_info = self.config[FILE_HEADER_INFO]

        (reqmeta, optmeta, updatemeta) = fmutils.create_file_metadata_dict(ftype, self.config[FILETYPE_METADATA], sectlabel, file_header_info)

        #print "reqmeta =", reqmeta
        #print "======================================================================"
        #print "optmeta =", optmeta
        #print "======================================================================"
        #print "updatemeta =", updatemeta
        #print "======================================================================"
        #sys.exit(1)

        if reqmeta:
            metaspecs[WCL_REQ_META] = OrderedDict()

            # convert names from specs to wcl
            valspecs = [META_HEADERS, META_COMPUTE, META_WCL]
            wclspecs = [WCL_META_HEADERS, WCL_META_COMPUTE, WCL_META_WCL]
            for i in range(len(valspecs)):
                if valspecs[i] in reqmeta:
                    metaspecs[WCL_REQ_META][wclspecs[i]] = reqmeta[valspecs[i]]

        if optmeta:
            metaspecs[WCL_OPT_META] = OrderedDict()

            # convert names from specs to wcl
            valspecs = [META_HEADERS, META_COMPUTE, META_WCL]
            wclspecs = [WCL_META_HEADERS, WCL_META_COMPUTE, WCL_META_WCL]
            for i in range(len(valspecs)):
                if valspecs[i] in optmeta:
                    metaspecs[WCL_OPT_META][wclspecs[i]] = optmeta[valspecs[i]]

        #print 'keys = ', metaspecs.keys()
        if updatefits:
            if updatemeta:
                updatemeta[WCL_UPDATE_WHICH_HEAD] = '0'  # framework always updates primary header
                metaspecs[WCL_UPDATE_HEAD_PREFIX+'0'] = updatemeta               
        elif updatemeta is not None:
            print "WARNING:  create_file_metadata_dict incorrectly returned values to update."
            print "\tContinuing but not updating these values."
            print "\tReport this to code developer."
            print "\t\t", updatemeta
            updatemeta = None

        # return None if no metaspecs
        if len(metaspecs) == 0:
            metaspecs = None

        return metaspecs




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
                    (path, filedict['filename'], filedict['compression']) = parse_filename(filelist[f]['fullname'], 7)  
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
        dbq = "select filename from genfile where filename in ('%s')" % "','".join(filenames)
        curs = self.cursor()
        curs.execute(dbq)
        havelist = []
        for row in curs:
            havelist.append(row[0])
        return havelist 

    def is_file_in_archive(self, fnames, filelist, args):
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


    def _get_required_metadata_headers (self, filetypes = None):
        """
        Return the metadata headers for the indicated filetype(s).

        The filetypes argument may be None (or omitted) to retrieve headers for
        all types, a string containing a single filetype, or a sequence of
        filetypes.  Filetypes are case insensitive.

        In all (successful) cases, the return value is a dictionary with the
        following structure:
            {<str:filetype>: {'filename_pattern'    : <str:filename pattern>,
                              'ops_dir_pattern'     : <str:dir pattern>,
                              'derived_header_names': [<str:name>...],
                              'other_header_names'  : [<str:name>...]
                             }
            }
        Note that either of the derived_header_names or other_header_names
        lists may be empty, but filetypes that have neither will not be
        retrieved from the database.

        """

        bindstr = self.get_positional_bind_string ()

        if filetypes is None:
            args  = []
            where = ''
        elif type (filetypes) in (str, unicode):
            args  = [ filetypes.lower () ]
            where = 'WHERE LOWER (r.filetype) = ' + bindstr
        else: # Allow any sort of sequence
            args  = [ f.lower () for f in filetypes ]
            s     = ','.join ([bindstr for b in args])
            where = 'WHERE LOWER (r.filetype) IN (' + s + ')'

        # Note that ORDER BY isn't really necessary here, but it stablizes
        # the header name lists so that callers will operate consistently.
        stmt = ("SELECT t.filetype, t.filename_pattern, t.ops_dir_pattern,"
                "       file_header_name, m.derived "
                "FROM   filetype t"
                "       JOIN required_metadata r ON r.filetype = t.filetype"
                "       JOIN metadata m USING (file_header_name) "
                ) + where + " ORDER BY t.filetype, file_header_name"

        cursor = self.cursor ()

        cursor.execute (stmt, args)

        retval = OrderedDict()
        for row in cursor.fetchall ():
            ftype = row [0]
            if ftype not in retval:
                retval [ftype] = {'filename_pattern'    : row [1],
                                  'ops_dir_pattern'     : row [2],
                                  'derived_header_names': [],
                                  'other_header_names'  : []
                                 }

            if row [4] == 1:
                retval [ftype] ['derived_header_names'].append (row [3])
            else:
                retval [ftype] ['other_header_names'].append (row [3])

        cursor.close ()

        # The file_header_name column is case sensitive in the database, but
        # header names are meant to be case insensitive; this can lead to
        # duplicate header names in the database.  In addition, various mis-
        # configuration of the metadata mapping tables could lead to duplicate
        # rows returned from the query above.  Check for this problem.

        for ftype in retval:
            hdrs = {hdr for hdr in retval [ftype]['derived_header_names'] +
                                   retval [ftype]['other_header_names']}
            if len ({hdr.lower () for hdr in hdrs}) != len (hdrs):
                raise DuplicateDBHeaderError ()

        # The filetype column in the filetype table is case sensitive in the
        # database, but this method forces case insensitive matching.  This
        # could lead to multiple filetypes being returned for a single
        # requested filetype.  Check for this.

        if len ({ftype.lower () for ftype in retval}) != len (retval):
            raise DuplicateDBFiletypeError ()

        return retval

    def _get_metadata_id_from_filename (self, filename):
        """
        Create a unique identifier for the metadata row for the specified file.

        The current implementation extracts the next value from the
        location_seq sequence in the database; however, a standalone algorithm
        taking the filename as input is expected and will ultimately replace
        this implementation.
        """

        return self.get_seq_next_value ('location_seq')

    def _get_filetype_metadata_map (self, filetype):
        """
        Retrieve the metadata to table and column mapping for a filetype.

        The returned dictionary contains two keys:
            table       value is name of the database table for the filetype
            id_column   value is name of id column for the table
            hdr_to_col  value is a dictionary mapping metadata header name to
                        database column name
        """

        tab, idcol = self._get_filetype_metadata_table (filetype)

        fmap = {'table': tab, 'id_column': idcol, 'hdr_to_col': {}}

        cursor = self.cursor ()
        bindstr = self.get_positional_bind_string ()

        stmt = ("SELECT file_header_name, m.column_name "
                "FROM   metadata m "
                "       JOIN required_metadata r USING (file_header_name) "
                "WHERE  LOWER (r.filetype) = " + bindstr)

        cursor.execute (stmt, (filetype.lower (), ))

        for row in cursor:
            fmap ['hdr_to_col'][row [0]] = row [1]

        return fmap

    def _get_filetype_metadata_table (self, filetype):
        """
        Retrieve the metadata table name and id column name for the specified
        filetype.
 
        Filetypes are considered case insensitive, but may appear multiple
        times with different case in the database.  This condition is detected
        and reported.  Other mis-configurations of the metadata mapping tables
        may lead to this report as well, however.
        """

        cursor  = self.cursor ()
        bindstr = self.get_positional_bind_string ()

        stmt = ("SELECT f.metadata_table, LOWER (m.id_column) "
                "FROM   filetype f "
                "       JOIN metadata_table m "
                "           ON m.table_name = f.metadata_table "
                "WHERE  LOWER (f.filetype) = " + bindstr)

        try:
            cursor.execute (stmt, (filetype.lower (), ))
            res = cursor.fetchall ()
        finally:
            cursor.close ()

        if len (res) == 1:
            return res [0][0], res [0][1]
        elif len (res) == 0:
            return None, None
        else:
            raise DuplicateDBFiletypeError ()

    def _metadata_ingest (self, filetype, metadata_by_filename):
        """
        Insert metadata from files of a particular type.

        The filetype argument is case insensitive.

        The metadata_by_filename argument is a dictionary of metadata indexed
        by source filename.  The filename will be used to generate a primary
        key for the file's metadata row.  The metadata for each file is
        specified as a dictionary indexed by metadata header.  The header names
        are case insensitive.

        The return value is a dictionary identifying certain types of problems
        with the following keys:
            bad_filetypes   list of bad filetypes (at most one).
            bad_file_ids    list of filenames for which ids could not be made
            missing_hdrs    dict of lists of missing headers per filename
            extra_hdrs      dict of lists of extra headers per filename
            duplicate_hdrs  dict of lists of duplicate headers per filename
        The keys are always present, but the values are non-empty only for the
        indicated conditions.

        Headers are considered duplicate if they are different only by case, so
        such duplication can exist in the metadata_by_filename parameter and is
        reported when detected.

        Metadata is not ingested for any files listed in bad_file_ids or
        duplicate_hdrs.  No metadata was ingested if bad_filetypes is not
        empty.
        """

        retval = {'bad_filetypes' : [],
                  'bad_file_ids'  : [],
                  'missing_hdrs'  : {},
                  'extra_hdrs'    : {},
                  'duplicate_hdrs': {}
                 }

        if not hasattr (self, 'md_map_cache'):
            # Create a cache so that information for a file type need by
            # collected from the database only once.
            self.md_map_cache = {}

        if filetype not in self.md_map_cache:
            # Haven't seen the filetype yet; get its map.
            fmap = self._get_filetype_metadata_map (filetype)
            self.md_map_cache [filetype] = fmap

        fmap = self.md_map_cache [filetype]

        if not fmap ['table']:
            # Specified filetype doesn't exist or doesn't have a table; punt
            retval ['bad_filetypes'].append (filetype)
            return retval

        # Using positional bind strings means that the columns and values need
        # to be in the same order, so construct a list of columns and and a
        # list of headers that are in the same order.  Start with "id" since
        # that must be added, but shouldn't be in the database.
        columns  = [fmap ['id_column']]
        hdr_list = ['id']
        for hdr, col in fmap ['hdr_to_col'].items ():
            columns.append (col)
            h = hdr.lower()
            if h == 'id':
                raise IdMetadataHeaderError ()
            hdr_list.append (h)

        # Make a set of expected headers for easy comparison to provided
        # headers.
        expected_hdrs = {h for h in hdr_list}

        if len (expected_hdrs) != len (hdr_list):
            raise DuplicateDBHeaderError ()

        expected_hdrs.discard ('id')

        # Loop through the provided files, adding a row for each.

        rows = []

        for filename, metadata in metadata_by_filename.items ():
            # Construct a copy of the metadata for this filename that uses
            # lowercase keys to implement case insenstive matching.

            mdLow         = {k.lower (): v for k, v in metadata.items ()}
            provided_hdrs = {hdr for hdr in mdLow}

            if len (provided_hdrs) != len (metadata):
                # Construct a list of tuples which identify the duplicated
                # headers.
                lowToGiven = {hdr: [] for hdr in provided_hdrs}
                for hdr in metadata:
                    lowToGiven [hdr.lower ()].append (hdr)

                duphdrs = []
                for val in lowToGiven.values ():
                    if len(val) > 1:
                        duphdrs.append (tuple (sorted (val)))

                retval ['duplicate_hdrs'][filename] = duphdrs
                continue

            # Record any issues with this file.

            extra_hdrs    = provided_hdrs - expected_hdrs
            missing_hdrs  = expected_hdrs - provided_hdrs

            if extra_hdrs:
                retval ['extra_hdrs'][filename] = sorted (list (extra_hdrs))
            if missing_hdrs:
                retval ['missing_hdrs'][filename] = sorted (list (missing_hdrs))

            fid = self._get_metadata_id_from_filename (filename)

            if fid is None:
                retval ['bad_file_ids'].append (filename)
            else:
                # Construct a row for this file and add to the list of rows.

                row = [fid if h == 'id' else mdLow.get (h) for h in hdr_list]

                rows.append (row)

        # If there're any rows, insert them.
        if rows:
            self.insert_many (fmap ['table'], columns, rows)

        return retval

    def _get_required_headers(self, filetypeDict):
        """
        For use by ingest_file_metadata. Collects the list of required header values.
        """
        REQUIRED = "r"
        allReqHeaders = set()
        for category,catDict in filetypeDict[REQUIRED].iteritems():
            allReqHeaders = allReqHeaders.union(catDict.keys())
        return allReqHeaders


    def _get_column_map(self, filetypeDict):
        """
        For use by ingest_file_metadata. Creates a lookup from column to header.
        """
        columnMap = OrderedDict()
        for statusDict in filetypeDict.values():
            if type(statusDict) in (OrderedDict,dict):
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
        if ftype.lower() in self.config[FILETYPE_METADATA]:
            return True
        else:
            return False

    def is_valid_archive(self, arname):
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
