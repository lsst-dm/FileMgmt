#!/usr/bin/env python

# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
    Extend core DM db class with functionality for managing files
    (metadata ingestion, "location" registering)
"""

__version__ = "$Rev$"

import os
import re
import sys
import argparse
from collections import OrderedDict

from intgutils.wcl import WCL
import despydmdb.desdmdbi as desdmdbi
import despymisc.miscutils as miscutils
import filemgmt.disk_utils_local as diskutils
import despymisc.provdefs as provdefs
import filemgmt.filemgmt_defs as fmdefs
import filemgmt.errors as fmerrs

class FileMgmtDB(desdmdbi.DesDmDbi):
    """
        Extend core DM db class with functionality for managing files
        (metadata ingestion, "location" registering)
    """

    ###########################################################################
    @staticmethod
    def requested_config_vals():
        """ return dictionary describing what values this class uses along with
            whether they are optional or required """
        return {'use_db':'opt', 'archive':'req', fmdefs.FILE_HEADER_INFO:'opt',
                'filetype_metadata':'req', 'des_services':'opt', 'des_db_section':'req'}

    ###########################################################################
    def __init__(self, initvals=None, fullconfig=None):

        if not miscutils.use_db(initvals):
            miscutils.fwdie("Error:  FileMgmtDB class requires DB but was told not to use DB", 1)

        self.desservices = None
        if 'des_services' in initvals:
            self.desservices = initvals['des_services']

        self.section = None
        if 'des_db_section' in initvals:
            self.section = initvals['des_db_section']
        elif 'section' in initvals:
            self.section = initvals['section']

        try:
            desdmdbi.DesDmDbi.__init__(self, self.desservices, self.section)
        except Exception as err:
            miscutils.fwdie(("Error: problem connecting to database: %s\n"
                             "\tCheck desservices file and environment variables") % err, 1)

        # precedence - db, file, params
        self.config = WCL()

        if miscutils.checkTrue('get_db_config', initvals, False):
            self._get_config_from_db()

        if 'wclfile' in initvals and initvals['wclfile'] is not None:
            fileconfig = WCL()
            with open(initvals['wclfile'], 'r') as fh:
                fileconfig.read(fh)
                self.config.update(fileconfig)

        if fullconfig is not None:
            self.config.update(fullconfig)
        self.config.update(initvals)

        self.filetype = None
        self.ftmgmt = None


    ###########################################################################
    def _get_config_from_db(self):
        """ reads some configuration values from the database """
        self.config = WCL()
        self.config['archive'] = self.get_archive_info()
        self.config['filetype_metadata'] = self.get_all_filetype_metadata()
        self.config[fmdefs.FILE_HEADER_INFO] = self.query_results_dict('select * from OPS_FILE_HEADER', 'name')


    ###########################################################################
    def register_file_in_archive(self, filelist, archive_name):
        """ Saves filesystem information about file like relative path
            in archive, compression extension, etc """
        # assumes files have already been declared to database (i.e., metadata)
        # caller of program must have already verified given filelist matches given archive
        # if giving fullnames, must include archive root
        # keys to each file dict must be lowercase column names, missing data must be None


        if miscutils.fwdebug_check(6, 'FILEMGMT_DEBUG'):
            miscutils.fwdebug_print("filelist = %s" % filelist)
        archivedict = self.config['archive'][archive_name]
        archiveroot = archivedict['root']

        origfilelist = filelist
        if isinstance(origfilelist, str):
            filelist = [origfilelist]

        if len(filelist) != 0:
            insfilelist = []
            for onefile in filelist:
                nfiledict = {}
                nfiledict['archive_name'] = archive_name
                if isinstance(onefile, dict):
                    if 'filename' in onefile and 'path' in onefile and 'compression' in onefile:
                        nfiledict['filename'] = onefile['filename']
                        nfiledict['compression'] = onefile['compression']
                        path = onefile['path']
                    elif 'fullname' in onefile:
                        parsemask = miscutils.CU_PARSE_PATH | miscutils.CU_PARSE_FILENAME | miscutils.CU_PARSE_COMPRESSION
                        (path, nfiledict['filename'], nfiledict['compression']) = miscutils.parse_fullname(onefile['fullname'], parsemask)
                    else:
                        miscutils.fwdie("Error:   Incomplete info for a file to register.   Given %s" % onefile, 1)
                elif isinstance(onefile, str):  # fullname
                    parsemask = miscutils.CU_PARSE_PATH | miscutils.CU_PARSE_FILENAME | miscutils.CU_PARSE_COMPRESSION
                    (path, nfiledict['filename'], nfiledict['compression']) = miscutils.parse_fullname(onefile, parsemask)


                # make sure compression starts with .
                if nfiledict['compression'] is not None and not re.match(r'^\.', nfiledict['compression']):
                    nfiledict['compression'] = '.' + nfiledict['compression']

                if re.match('^/', path):   # if path is absolute
                    if miscutils.fwdebug_check(3, 'FILEMGMT_DEBUG'):
                        miscutils.fwdebug_print("absolute path = %s" % path)
                        miscutils.fwdebug_print("archiveroot = %s/" % archiveroot)

                    # get rid of the archive root from the path to store
                    if re.match('^%s/' % archiveroot, path):
                        nfiledict['path'] = path[len(archiveroot)+1:]
                    else:
                        canon_archroot = os.path.realpath(archiveroot)
                        canon_path = os.path.realpath(path)

                        # get rid of the archive root from the path to store
                        if re.match('^%s/' % canon_archroot, canon_path):
                            nfiledict['path'] = canon_path[len(canon_archroot)+1:]
                        else:
                            miscutils.fwdie(("Error: file's absolute path (%s) does not "
                                             "contain the archive root (%s) (filedict:%s)") % \
                                            (path, archiveroot, nfiledict), 1)
                else:
                    if miscutils.fwdebug_check(3, 'FILEMGMT_DEBUG'):
                        miscutils.fwdebug_print("relative path = %s" % path)
                    nfiledict['path'] = path # assume only contains the relative path within the archive

                insfilelist.append(nfiledict)

            #colnames = ['filename', 'filesize', 'compression', 'path', 'archive_name']
            colnames = ['filename', 'compression', 'path', 'archive_name']
            try:
                self.insert_many_indiv('FILE_ARCHIVE_INFO', colnames, insfilelist)
            except:
                print "Error from insert_many_indiv in register_file_archive"
                print "colnames =", colnames
                print "filelist =", insfilelist
                raise

    ###########################################################################
    def has_metadata_ingested(self, filetype, fullnames):
        """ Check whether metadata has been ingested for given file """

        self.dynam_load_ftmgmt(filetype)

        listfullnames = fullnames
        if isinstance(fullnames, str):
            listfullnames = [fullnames]
        
        results = self.ftmgmt.has_metadata_ingested(listfullnames)

        if isinstance(fullnames, str):
            results = results[fullnames]
        return results

    ###########################################################################
    def check_valid(self, filetype, fullnames):
        """ Check whether file is a valid file for the given filetype """

        self.dynam_load_ftmgmt(filetype)

        listfullnames = fullnames
        if isinstance(fullnames, str):
            listfullnames = [fullnames]
        
        results = self.ftmgmt.check_valid(listfullnames)

        if isinstance(fullnames, str):
            results = results[fullnames]

        return results

    ###########################################################################
    def has_contents_ingested(self, filetype, fullnames):
        """ Check whether metadata has been ingested for given files """

        self.dynam_load_ftmgmt(filetype)

        listfullnames = fullnames
        if isinstance(fullnames, str):
            listfullnames = [fullnames]
        
        results = self.ftmgmt.has_contents_ingested(listfullnames)

        if isinstance(fullnames, str):
            results = results[fullnames]

        return results

    ######################################################################
    def ingest_contents(self, filetype, fullnames):
        """ Call filetype specific function to ingest contents """

        listfullnames = fullnames
        if isinstance(fullnames, str):
            listfullnames = [fullnames]
        
        results = self.has_contents_ingested(filetype, listfullnames)
        newlist = [fname for fname in results if not results[fname]]

        self.dynam_load_ftmgmt(filetype)
        self.ftmgmt.ingest_contents(newlist)

    ###########################################################################
    def is_file_in_archive(self, filelist, archive_name):
        """ Checks whether given files are in the specified archive according to the DB """
        # TODO change to return count(*) = 0 or 1 which would preserve array
        #      another choice is to return path, but how to make it return null for path that doesn't exist

        gtt_name = self.load_filename_gtt(filelist)

        # join to GTT_FILENAME for query
        sql = ("select filename||compression from %(gtt)s g where exists "
               "(select filename from file_archive_info fai where "
               "fai.archive_name=%(ar)s and fai.filename=g.filename)") % \
               ({'ar':self.get_named_bind_string('archive_name'), 'gtt':gtt_name})
        if miscutils.fwdebug_check(3, 'FILEMGMT_DEBUG'):
            miscutils.fwdebug_print("sql = %s" % sql)

        curs = self.cursor()
        curs.execute(sql, {'archive_name': archive_name})
        existslist = []
        for row in curs:
            existslist.append(row[0])
        return existslist


    ###########################################################################
    def _get_required_headers(self, filetype_dict):
        """
        For use by ingest_file_metadata. Collects the list of required header values.
        """
        REQUIRED = "r"
        allReqHeaders = set()
        for hdu_dict in filetype_dict['hdus'].values():
            if REQUIRED in hdu_dict:
                for cat_dict in hdu_dict[REQUIRED].values():
                    allReqHeaders = allReqHeaders.union(cat_dict.keys())
        return allReqHeaders


    ###########################################################################
    def _get_column_map(self, filetype_dict):
        """
        For use by ingest_file_metadata. Creates a lookup from column to header.
        """
        columnMap = OrderedDict()
        for hdu_dict in filetype_dict['hdus'].values():
            for status_dict in hdu_dict.values():
                for cat_dict in status_dict.values():
                    for header, columns in cat_dict.iteritems():
                        collist = columns.split(',')
                        for position, column in enumerate(collist):
                            if len(collist) > 1:
                                columnMap[column] = header + ":" + str(position)
                            else:
                                columnMap[column] = header
        return columnMap


    ###########################################################################
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
        dbdict = self.config[fmdefs.FILETYPE_METADATA]
        FILETYPE = "filetype"
        FILENAME = "filename"
        METATABLE = "metadata_table"
        COLMAP = "column_map"
        ROWS = "rows"
        metadataTables = OrderedDict()
        fullmessage = ""

        try:
            for key, filedata in filemeta.iteritems():
                if not isinstance(filedata, dict):
                    print "Invalid type for filedata:", type(filedata)
                    print filedata
                    
                if FILENAME not in filedata.keys():
                    fullmessage = '\n'.join(["ERROR: cannot upload file <" + key + ">, no FILENAME provided.", fullmessage])
                    continue
                if FILETYPE not in filedata.keys():
                    fullmessage = '\n'.join(["ERROR: cannot upload file " + filedata[FILENAME] + ": no FILETYPE provided.", fullmessage])
                    continue
                if filedata[FILETYPE] not in dbdict:
                    fullmessage = '\n'.join(["ERROR: cannot upload " + filedata[FILENAME] + ": " + \
                            filedata[FILETYPE] + " is not a known FILETYPE.", fullmessage])
                    continue
                # check that all required are present
                allReqHeaders = self._get_required_headers(dbdict[filedata[FILETYPE]])
                for dbkey in allReqHeaders:
                    if dbkey not in filedata.keys() or filedata[dbkey] == "":
                        fullmessage = '\n'.join(["ERROR: " + filedata[FILENAME] + " missing required data for " + dbkey, fullmessage])

                # any error should then skip all upload attempts
                if fullmessage:
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

            for metaTable, metadict in metadataTables.iteritems():
                #self.insert_many(metaTable, metadict[COLMAP].keys(), metadict[ROWS])
                self.insert_many_indiv(metaTable, metadict[COLMAP].keys(), metadict[ROWS])
            #self.commit()

        except Exception as ex:
            #raise FileMetadataIngestError(ex)
            raise

        if fullmessage:
            print >> sys.stderr, fullmessage
            raise fmerrs.RequiredMetadataMissingError(fullmessage)

    # end ingest_file_metadata


    ###########################################################################
    def is_valid_filetype(self, ftype):
        """ Checks filetype definitions to determine if given filetype exists """
        if ftype.lower() in self.config[fmdefs.FILETYPE_METADATA]:
            return True
        else:
            return False

    ###########################################################################
    def is_valid_archive(self, arname):
        """ Checks archive definitions to determine if given archive exists """
        if arname.lower() in self.config['archive']:
            return True
        else:
            return False


    ###########################################################################
    def get_file_location(self, filelist, arname, compress_order=fmdefs.FM_PREFER_COMPRESSED):
        """ Return relative archive paths and filename including any compression extenstion """

        fileinfo = self.get_file_archive_info(filelist, arname, compress_order)
        rel_filenames = {}
        for fname, finfo in fileinfo.items():
            rel_filenames[fname] = finfo['rel_filename']
        return rel_filenames


    ###########################################################################
    def get_file_archive_info(self, filelist, arname, compress_order=fmdefs.FM_PREFER_COMPRESSED):
        """ Return information about file stored in archive (e.g., filename, size, rel_filename, ...) """

        # sanity checks
        if 'archive' not in self.config:
            miscutils.fwdie('Error: Missing archive section in config', 1)

        if arname not in self.config['archive']:
            miscutils.fwdie('Error: Invalid archive name (%s)' % arname, 1)

        if 'root' not in self.config['archive'][arname]:
            miscutils.fwdie('Error: Missing root in archive def (%s)' % self.config['archive'][arname], 1)

        if not isinstance(compress_order, list):
            miscutils.fwdie('Error:  Invalid compress_order.  It must be a list of compression extensions (including None)', 1)

        # query DB getting all files regardless of compression
        #     Can't just use 'in' expression because could be more than 1000 filenames in list
        #           ORA-01795: maximum number of expressions in a list is 1000

        # insert filenames into filename global temp table to use in join for query
        gtt_name = self.load_filename_gtt(filelist)

        # join to GTT_FILENAME for query
        sql = ("select genfile.filetype,fai.path,fai.filename,fai.compression, "
               "art.filesize, art.md5sum from genfile,file_archive_info fai,opm_artifact art, "
               "%(gtt)s where fai.archive_name=%(ar)s and genfile.filename=%(gtt)s.filename and "
               "fai.filename=%(gtt)s.filename and art.name=fai.filename and "
               "(art.compression=fai.compression or "
               "(art.compression is NULL and fai.compression is NULL))") % \
              ({'ar':self.get_named_bind_string('archive_name'), 'gtt':gtt_name})
        curs = self.cursor()
        curs.execute(sql, {'archive_name': arname})
        desc = [d[0].lower() for d in curs.description]

        fullnames = {}
        for comp in compress_order:
            fullnames[comp] = {}

        for line in curs:
            d = dict(zip(desc, line))

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


    ###########################################################################
    def get_file_archive_info_path(self, path, arname, compress_order=fmdefs.FM_PREFER_COMPRESSED):
        """ Return information about file stored in archive (e.g., filename, size, rel_filename, ...) """

        # sanity checks
        if 'archive' not in self.config:
            miscutils.fwdie('Error: Missing archive section in config', 1)

        if arname not in self.config['archive']:
            miscutils.fwdie('Error: Invalid archive name (%s)' % arname, 1)

        if 'root' not in self.config['archive'][arname]:
            miscutils.fwdie('Error: Missing root in archive def (%s)' % self.config['archive'][arname], 1)

        if not isinstance(compress_order, list):
            miscutils.fwdie('Error:  Invalid compress_order.  It must be a list of compression extensions (including None)', 1)

        likestr = self.get_regex_clause('path', '%s/.*' % path)

        # query DB getting all files regardless of compression
        sql = ("select filetype,file_archive_info.* from genfile,file_archive_info "
               "where archive_name='%s' and genfile.filename=file_archive_info.filename "
               "and %s") % (arname, likestr)
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

    ######################################################################
    def dynam_load_ftmgmt(self, filetype):
        """ Dynamically load a filetype mgmt class """

        if miscutils.fwdebug_check(3, 'FILEMGMT_DEBUG'):
            miscutils.fwdebug_print("filetype = %s" % self.filetype)

        if self.filetype is None or filetype != self.filetype or \
           self.ftmgmt is None:
            classname = 'filemgmt.ftmgmt_generic.FtMgmtGeneric'
            if filetype in self.config['filetype_metadata']:
                if 'filetype_mgmt' in self.config['filetype_metadata'][filetype] and \
                      self.config['filetype_metadata'][filetype]['filetype_mgmt'] is not None:
                    classname = self.config['filetype_metadata'][filetype]['filetype_mgmt']
                else:
                    miscutils.fwdie('Error: Invalid filetype (%s)' % filetype, 1)

            # dynamically load class for the filetype
            filetype_mgmt = None
            filetype_mgmt_class = miscutils.dynamically_load_class(classname)
            try:
                filetype_mgmt = filetype_mgmt_class(filetype, self, self.config)
            except Exception as err:
                print "ERROR\nError: creating filemgmt object\n%s" % err
                raise
            
            self.filetype = filetype
            self.ftmgmt = filetype_mgmt


    ######################################################################
    def register_file_data(self, ftype, fullnames, wgb_task_id,
                           do_update, update_info=None):
        """ Save artifact, metadata, wgb provenance, and simple contents for given files """

        self.dynam_load_ftmgmt(ftype)

        results = {}
        for fname in fullnames:
            metadata = {}
            fileinfo = {}
            
            metadata = self.ftmgmt.perform_metadata_tasks(fname, do_update, update_info)
            fileinfo = diskutils.get_single_file_disk_info(fname,
                                                           save_md5sum=True,
                                                           archive_root=None)
            basename = fileinfo['filename']
            if fileinfo['compression'] is not None:
                basename += fileinfo['compression']    

            has_metadata = self.has_metadata_ingested(ftype, fname)
            if not has_metadata:
                self.save_file_info([fileinfo], 
                                    {'file_1': metadata},
                                    {provdefs.PROV_WGB:{'exec_1': basename}},
                                    {'exec_1': wgb_task_id})
            elif miscutils.fwdebug_check(0, 'FILEMGMT_DEBUG'):
                miscutils.fwdebug_print("INFO: %s already has metadata ingested" % fname)

            has_contents = self.ftmgmt.has_contents_ingested([fname])
            if not has_contents[fname]:
                self.ftmgmt.ingest_contents([fname])
            elif miscutils.fwdebug_check(0, 'FILEMGMT_DEBUG'):
                miscutils.fwdebug_print("INFO: %s already has contents ingested" % fname)
            results[fname] = { 'diskinfo': fileinfo, 'metadata': metadata } 
        return results



    ######################################################################
    def save_file_info(self, artifacts, metadata, prov, execids):
        """ save non-location information about file """

        if miscutils.fwdebug_check(3, 'FILEMGMT_DEBUG'):
            miscutils.fwdebug_print("artifacts = %s" % artifacts)
            miscutils.fwdebug_print("metadata = %s" % metadata)
            miscutils.fwdebug_print("prov = %s" % prov)
            miscutils.fwdebug_print("execids = %s" % execids)

        if artifacts is not None and len(artifacts) > 0:
            self.create_artifacts(artifacts)

        if metadata is not None and len(metadata) > 0:
            self.ingest_file_metadata(metadata)

        if prov is not None and len(prov) > 0:
            self.ingest_provenance(prov, execids)


    ###########################################################################
    def create_artifacts(self, filelist):
        """ create artifact records for any files that require them """
        # filelist is list of file dictionaries

        gtt_name = self.load_artifact_gtt(filelist)

        sqlstr = """
            insert into OPM_ARTIFACT (name, compression, filesize, md5sum)
            select filename, compression, filesize, md5sum
            from %s gt
            where not exists(
                select * from opm_artifact a2 where a2.name=gt.filename and
                NVL(a2.compression,'1') = NVL(gt.compression,'1')
            )""" % gtt_name
        cursor = self.cursor()
        try:
            cursor.execute(sqlstr)
        except Exception as exc:
            print "Error: problems creating artifacts"
            print "       %s\n" % exc
            print "Values trying to load:"
            print filelist
            raise

    ###########################################################################
    def getFilenameIdMap(self, prov):
        """ Return a mapping of filename to artifact id """

        if miscutils.fwdebug_check(6, 'FILEMGMT_DEBUG'):
            miscutils.fwdebug_print("prov = %s" % prov)

        allfiles = set()
        if provdefs.PROV_USED in prov:
            for filenames in prov[provdefs.PROV_USED].values():
                for fname in filenames.split(provdefs.PROV_DELIM):
                    allfiles.add(fname.strip())
        if provdefs.PROV_WGB in prov:
            for filenames in prov[provdefs.PROV_WGB].values():
                for fname in filenames.split(provdefs.PROV_DELIM):
                    allfiles.add(fname.strip())
        if provdefs.PROV_WDF in prov:
            for tuples in prov[provdefs.PROV_WDF].values():
                for filenames in tuples.values():
                    for fname in filenames.split(provdefs.PROV_DELIM):
                        allfiles.add(fname.strip())

        result = []
        if len(allfiles) > 0:
            # build a map between filenames (with compression extension) and artifact ID
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


    ###########################################################################
    def ingest_provenance(self, prov, execids):
        """ Save provenance to OPM tables """

        insert_sql = """insert into %s d (%s) select %s,%s %s where not exists(
                    select * from %s n where n.%s=%s and n.%s=%s)"""

        data = []
        bind_str = self.get_positional_bind_string()
        cursor = self.cursor()
        filemap = self.getFilenameIdMap(prov)
        if miscutils.fwdebug_check(6, 'FILEMGMT_DEBUG'):
            miscutils.fwdebug_print("filemap = %s" % filemap)

        if provdefs.PROV_USED in prov:
            for execname, filenames in prov[provdefs.PROV_USED].iteritems():
                for fname in filenames.split(provdefs.PROV_DELIM):
                    rowdata = []
                    rowdata.append(execids[execname])
                    rowdata.append(filemap[fname.strip()])
                    rowdata.append(execids[execname])
                    rowdata.append(filemap[fname.strip()])
                    data.append(rowdata)
            exec_sql = insert_sql % (fmdefs.PROV_USED_TABLE,
                                     fmdefs.PROV_TASK_ID + "," + fmdefs.OPM_ARTIFACT_ID,
                                     bind_str, bind_str, self.from_dual(),
                                     fmdefs.PROV_USED_TABLE, fmdefs.PROV_TASK_ID, bind_str,
                                     fmdefs.OPM_ARTIFACT_ID, bind_str)
            cursor.executemany(exec_sql, data)
            data = []

        if provdefs.PROV_WGB in prov:
            for execname, filenames in prov[provdefs.PROV_WGB].iteritems():
                for fname in filenames.split(provdefs.PROV_DELIM):
                    rowdata = []
                    rowdata.append(execids[execname])
                    rowdata.append(filemap[fname.strip()])
                    rowdata.append(execids[execname])
                    rowdata.append(filemap[fname.strip()])
                    data.append(rowdata)
            exec_sql = insert_sql % (fmdefs.PROV_WGB_TABLE,
                                     fmdefs.PROV_TASK_ID + "," + fmdefs.OPM_ARTIFACT_ID,
                                     bind_str, bind_str, self.from_dual(),
                                     fmdefs.PROV_WGB_TABLE, fmdefs.PROV_TASK_ID, bind_str,
                                     fmdefs.OPM_ARTIFACT_ID, bind_str)
            cursor.executemany(exec_sql, data)
            data = []

        if provdefs.PROV_WDF in prov:
            for tuples in prov[provdefs.PROV_WDF].values():
                if provdefs.PROV_PARENTS not in tuples:
                    miscutils.fwdie("Error: missing %s in one of %s" % (provdefs.PROV_PARENTS, provdefs.PROV_WDF), fmdefs.FM_EXIT_FAILURE)
                elif provdefs.PROV_CHILDREN not in tuples:
                    miscutils.fwdie("Error: missing %s in one of %s" % (provdefs.PROV_CHILDREN, provdefs.PROV_WDF), fmdefs.FM_EXIT_FAILURE)
                else:
                    for parentfile in tuples[provdefs.PROV_PARENTS].split(provdefs.PROV_DELIM):
                        for childfile in tuples[provdefs.PROV_CHILDREN].split(provdefs.PROV_DELIM):
                            rowdata = []
                            rowdata.append(filemap[parentfile.strip()])
                            rowdata.append(filemap[childfile.strip()])
                            rowdata.append(filemap[parentfile.strip()])
                            rowdata.append(filemap[childfile.strip()])
                            data.append(rowdata)

            exec_sql = insert_sql % (fmdefs.PROV_WDF_TABLE,
                                     fmdefs.PARENT_OPM_ARTIFACT_ID + "," + fmdefs.CHILD_OPM_ARTIFACT_ID,
                                     bind_str, bind_str, self.from_dual(),
                                     fmdefs.PROV_WDF_TABLE, fmdefs.PARENT_OPM_ARTIFACT_ID,
                                     bind_str, fmdefs.CHILD_OPM_ARTIFACT_ID,
                                     bind_str)
            if len(data) > 0:
                cursor.executemany(exec_sql, data)
            else:
                miscutils.fwdebug_print("Warn: %s section given but had 0 valid entries" % (provdefs.PROV_WDF))
                
    #        self.commit()
    #end_ingest_provenance
