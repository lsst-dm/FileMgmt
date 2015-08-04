#!/usr/bin/env python

# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
Generic filetype management class used to do filetype specific tasks
     such as metadata and content ingestion
"""

__version__ = "$Rev$"

import json
from collections import defaultdict


from filemgmt.ftmgmt_generic import FtMgmtGeneric
import despydmdb.dmdb_defs as dmdbdefs
import despymisc.miscutils as miscutils
import filemgmt.fmutils as fmutils 

class FtMgmtSNManifest(FtMgmtGeneric):
    """  Base/generic class for managing a filetype (get metadata, update metadata, etc) """

    ######################################################################
    def __init__(self, filetype, dbh, config):
        """ Initialize object """
        # config must have filetype_metadata and file_header_info
        FtMgmtGeneric.__init__(self, filetype, dbh, config)

    ######################################################################
    def has_contents_ingested(self, fullnames):
        """ Check if file has contents ingested """

        listfullnames = fullnames
        if isinstance(fullnames, str):
            listfullnames = [fullnames]

        # assume uncompressed and compressed files have same metadata
        # choosing either doesn't matter
        byfilename = {}
        for fname in listfullnames:
            filename = miscutils.parse_fullname(fname, miscutils.CU_PARSE_FILENAME)
            byfilename[filename] = fname

        self.dbh.empty_gtt(dmdbdefs.DB_GTT_FILENAME)
        self.dbh.load_filename_gtt(byfilename.keys())

        dbq = "select m.manifest_filename from %s m, %s g where m.manifest_filename=g.filename" % \
                 ("MANIFEST_EXPOSURE", dmdbdefs.DB_GTT_FILENAME)
        curs = self.dbh.cursor()
        curs.execute(dbq)

        results = {}
        for row in curs:
            results[byfilename[row[0]]] = True

        for fname in listfullnames:
            if fname not in results:
                results[fname] = False

        self.dbh.empty_gtt(dmdbdefs.DB_GTT_FILENAME)

        if isinstance(fullnames, str):
            results = results[fullnames]
        return results


    ######################################################################
    def ingest_contents(self, fullname, **kwargs):
        """ reads json manifest file and ingest into the DB tables
            EXPOSURES_IN_MANIFEST and SN_SUBMIT_REQUEST values needed to
            determine arrival of exposures taken for a SN field."""

        allMandatoryExposureKeys = ['expid', 'object', 'date', 'acttime', 'filter']
        allExposures = fmutils.read_json_single(fullname, allMandatoryExposureKeys)
        self.ingestAllExposures(allExposures)


    ######################################################################
    def insert_dictionary_2Db(self, query, dictionary):
        """Execute a query and return a cursor to a query
        :param query: string with query statement
        :param dictionary: dictionary to use in query

        """

        try:
            cur = self.dbh.cursor()
            cur.execute(query, dictionary)
            if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
                miscutils.fwdebug_print("dictionary into database " % (dictionary))
            success = 1
        #except cx_Oracle.IntegrityError as e:
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if error.code == 955:
                print('Table already exists')
            elif error.code == 1031:
                print("Insufficient privileges")
            print(error.code)
            print(error.message)
            print(error.context)
            success = 0
            raise
        return success

    ######################################################################
    def ingestAllExposures(self, allExposures):
        """
        Ingest all the exposures in EXPOSURES_IN_MANIFEST and SN_SUBMIT_REQUEST

        #If SEQNUM is > 1, then it means the same field was taken again during the same night.
        #This will only happens in rare occasion when the sequence had to be aborted before it finished.

        :param allExposures: Dictionary with the following keys:
        [set_type,createdAt,expid,object,date,acttime,filter]

        """


        newdictionary = {}
        for key in ['CAMSYM', 'EXPNUM', 'MANIFEST_FILENAME', 'FIELD', 'BAND', 'EXPTIME', 'NITE']:
            newdictionary[key] = allExposures[key]

        #print "xx", allExposures
        dict2Ingest = {}
        for i in range(len(allExposures['EXPTIME'])):
            for key in newdictionary.keys():
                keytoingest = key
                valuetoingest = newdictionary[key][i]
                dict2Ingest[keytoingest] = valuetoingest
            if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
                miscutils.fwdebug_print("dict2Ingest %s " % (dict2Ingest))
            try:
                sqlInsertExposuresInManifest = """insert into MANIFEST_EXPOSURE (CAMSYM,EXPNUM,MANIFEST_FILENAME,NITE,FIELD,BAND,EXPTIME) VALUES
                                    (:CAMSYM, :EXPNUM, :MANIFEST_FILENAME, :NITE, :FIELD, :BAND, :EXPTIME)"""


                if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
                    miscutils.fwdebug_print("sqlInsertExposuresInManifest %s " % (sqlInsertExposuresInManifest))
                success = self.insert_dictionary_2Db(sqlInsertExposuresInManifest, dict2Ingest)

                if success and miscutils.fwdebug_check(1, 'FTMGMT_DEBUG'):
                    miscutils.fwdebug_print("Insert into EXPOSURES_IN_MANIFEST was successful..")

            except cx_Oracle.IntegrityError as e:
                print "error while inserting into EXPOSURES_IN_MANIFEST: ", e
                raise


        ########################################################################################
        #
        #Fix first expnum. First expnum is the first exposure for each filter set. In case of
        #one a field with one filter exposure, then first_expnum = expnum.
        #For more than one exposure / band/field, then first_expnum = first exposure of set.
        #

        #Determine index of list for exptime = 10. (poiting exposure)
        allexps = allExposures['EXPTIME']
        if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("all exptimes %s" % (allexps))
        for i, val in enumerate(allexps):
            if val == 10.0:
                pointingIndex = i

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("pointing Exposure index is %s" % pointingIndex)

        #find where there are repetead bands, but exclude the band where the exptime = 10
        ListofBands = allExposures['BAND']
        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("listOfaBands...%s" % ListofBands)

        bandsDicIndexes = defaultdict(list)

        for i, item in enumerate(allExposures['BAND']):
            bandsDicIndexes[item].append(i)

        #I have to loop trorugh the dictionary for all the bands. Cound how many bands. Get the vaues from this dictionary
        #which is the index to the list, and use that to determine the elementes for all the other dictionaries.
        #I need the follwoing elementsl 'FIELD','NITE','BAND','MANIFEST_FILENAME','FIRST_EXPNUM','SEQNUM'
        ind2use = []
        flag_first = 0
        for ind, b in enumerate(ListofBands):
            if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
                miscutils.fwdebug_print("indexes %s %s" % (bandsDicIndexes[b], ind))
            if ind == pointingIndex:
                if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
                    miscutils.fwdebug_print("found pointing index %s %s " % (ind, pointingIndex))
                continue
            else:
                #for two exposures and one of them is the poiting
                if len(bandsDicIndexes[b]) <= 2 and ind == pointingIndex+1:
                    ind2use.append((max(bandsDicIndexes[b])))
                    #print "the index", ind2use
                #if there are more than 2 exposures (generally for deep fields
                elif len(bandsDicIndexes[b]) > 2 and ind == pointingIndex+1:
                    ind2use.append(bandsDicIndexes[b][ind])
                    flag_first = 1
                elif len(bandsDicIndexes[b]) == 1:
                    ind2use.append(bandsDicIndexes[b][0])
                elif len(bandsDicIndexes[b]) == 2 and ind != pointingIndex and flag_first == 0:
                    ind2use.append(min(bandsDicIndexes[b]))
                    flag_first = 1
            if flag_first:
                break

        #contruct the dictionary with only the elements that needs to go into the DB
        #To do this use the ind2use extracted from the above list.
        newDic = {}
        for index in ind2use:
            #print index
            newDic['FIELD'] = allExposures['FIELD'][index]
            newDic['NITE'] = allExposures['NITE'][index]
            newDic['BAND'] = allExposures['BAND'][index]
            newDic['MANIFEST_FILENAME'] = allExposures['MANIFEST_FILENAME'][index]
            newDic['FIRST_EXPNUM'] = allExposures['EXPNUM'][index]
            newDic['SEQNUM'] = allExposures['SEQNUM'][index]
            if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
                miscutils.fwdebug_print("index=%s, newDic=%s" % (index, newDic))

            #Ingest into the database each of them
            try:
                sqlInsertSNSubmitRequest = """insert into SN_SUBMIT_REQUEST (FIELD,NITE,BAND,MANIFEST_FILENAME,FIRST_EXPNUM,SEQNUM) VALUES
                                            (:FIELD, :NITE, :BAND, :MANIFEST_FILENAME, :FIRST_EXPNUM, :SEQNUM)"""

                if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
                    miscutils.fwdebug_print("sqlInsertSNSubmitRequest = %s" % \
                                            sqlInsertExposuresInManifest)

                success = self.insert_dictionary_2Db(sqlInsertSNSubmitRequest, newDic)
                if success and miscutils.fwdebug_check(1, 'FTMGMT_DEBUG'):
                    miscutils.fwdebug_print("Insert into SN_SUBMIT_REQUEST was successful..")

            except cx_Oracle.IntegrityError as e:
                print "error while inserting into SN_SUBMIT_REQUEST: ", e
                raise

