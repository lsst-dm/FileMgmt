#!/usr/bin/env python

# $Id: desdbi.py 10292 2013-01-07 17:54:32Z mgower $
# $Rev:: 10292                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2013-01-07 11:54:32 #$:  # Date of last commit.

"""
    Provide a dialect-neutral interface to DES databases.

    Classes:
        DesDbi - Connects to a Postgresql or Oracle instance of a DES database
                 upon instantiation and the resulting object provides an
                 interface based on the Python DB API with extensions to allow
                 interaction with the database in a dialect-neutral manner.

    Developed at: 
    The National Center for Supercomputing Applications (NCSA).

    Copyright (C) 2011 Board of Trustees of the University of Illinois. 
    All rights reserved.

"""

__version__ = "$Rev: 10292 $"

import re
import sys
import copy
import coreutils.desdbi
import fileutils
from subprocess import call
import subprocess
import os.path

#import serviceaccess
# importing of DB specific modules done down inside code

#import coreutils.errors

class Cache(object):
    """
    need to write the description here
    """

    _dbh = coreutils.desdbi.DesDbi()
    _cur = _dbh.cursor()

    def internal_put():
        
        return status 

    def put_into_local_cache(input_list):
    
        for key in input_list:
            print key, 'corresponds to', input_list[key]
            mount_point = find_mount_point(input_item[key])
        return status

    def get_from_cache():

        return status

    def put_after_job(self,file_list,cache_name):
        "this is the new comment I am trying from within the function"
        return

    def put_within_job_wrapper(self,file_list,cache_name,site_name):
        """
        this a wrapper to put_within_job function. Its job is to make all the DB queries needed to make a call to the actual function
        inputs are an array of files to be moved to cache
        the name of the cache
        name of the site where this move is happening
        """
        colList = {}#['name','on_target','not_on_target','root','cache_name']
        returnCodeDict = {}
        sqlGetCacheInfo = "select * from ops_data_defs where type = 'cache' and name = '%s'" % cache_name
        resultDict = fileutils.get_queryresult_dictionay(sqlGetCacheInfo,self._cur)
       # print " the query sqlGetCacheInfo %s returned %s" % (sqlGetCacheInfo,resultDict)
        sqlGetSiteInfo = "select * from ops_data_defs where type = 'runsite' and name = '%s'" % site_name
        #print sqlGetSiteInfo
        resultSite = fileutils.get_queryresult_dictionay(sqlGetSiteInfo,self._cur)
        runsite_root = resultSite[0]['root']
        """
        the result dict has an array of one element in this case so just pick up the first one
        """
        cacheDict = resultDict[0]
        print "put within job inputs: file Dictionary"
        print file_list
        print "runsite_root: "
        print runsite_root
        print "cache Dictionary from Table: "
        print cacheDict
        self.put_within_job(file_list,runsite_root,cacheDict)


    def put_within_job(self,file_list,runsite_root,cacheDict):
        """ the function is used to put files into cache from within an already running job.
        A list is provided and each file is put into the cache directory. 
        the location table for cache directory is then inserted new rows or the previously existing rows are updated
        on successful transfer of the file
        """ 

        """create a regular expression for the root directory of the runsite. 
        this will be removed from the path of source file to get the relative path which will be created inside the cache directory
        """
        runsite_root_canonicalized = os.path.realpath(runsite_root)
        returnCodeDict = {}
        #print "the runsite is " + os.path.realpath(runsite_root)
        
        onTargetMethod = cacheDict['on_target']
        for file in file_list:
           # print "putting file" + file
            """if the source filepath is relative, convert it into absolute path
               we do this so that the absolute path can be easily used to copy files from one location to another 
               without confusion
            """
            if not os.path.isabs(os.path.realpath(file)):
                file = runsite_root + '/' + os.path.realpath(file)
            else:
                file = os.path.realpath(file)
            """create the absolute destination path for the location in cache for the incoming file.
             this is done by adding the root of cache location to the relative path of the source file"""
            #print "another attempt: " + os.path.realpath(file).replace(os.path.realpath(runsite_root),'')

            destination = cacheDict['root'] + os.path.realpath(file).replace(runsite_root_canonicalized,'')
            #print "the destination filepath is %s" % destination
            """
            call transportFile function and store the results in to a dictionary.
            this dictionary will be used later to update the database
            """
            fileName = os.path.basename(file)
            "initialize the returnCodeDict[fileName] dictionary"

            returnCodeDict[fileName] = {}
            returnCodeDict[fileName]['transfer_status'] = self.transportFile(file,destination,onTargetMethod)
            returnCodeDict[fileName]['filename'] = fileName
            returnCodeDict[fileName]['path'] = os.path.dirname(destination) #os.path.realpath(os.path.dirname(file))
            returnCodeDict[fileName]['cache_name'] = cacheDict['name']
            returnCodeDict[fileName]['site_name'] = cacheDict['sitename']
        
#        print "calling DB"
#        print returnCodeDict    
        self.logTransportedFilesInDB(returnCodeDict)
        return
    
    def logTransportedFilesInDB(self,fileDict):
        
        insertArr = []
        p = re.compile("(\.fits)(\.fz)?")
        for file, fileDets in fileDict.iteritems():
            #print "looping over" + file
            if fileDets['transfer_status'] is 0:
                file = p.sub("",file)
               # print "inserting file" + file
                insertArr.append([file,fileDets['path'],fileDets['site_name'],fileDets['cache_name']])
        #print "the insert arr"
        #print insertArr    

        sqlInsert = "insert into ops_cache_location  values (:1,:2,:3,:4) " 
        #sqlMerge = "merge into ops_cache_location A using (select :1 as file from dual ) B on ( A.filename = B.file) WHEN NOT MATCHED THEN INSERT  (A.filename, A.path, A.archive_node_name, A.cache_name values (:2,:3,:4,:5) "  # insertArr
        self._cur.executemany(sqlInsert,insertArr)
        self._dbh.commit()
        #print "the final insertArr is "
        #print insertArr    

    
    def transportFile(self,source,destination,method):
        """
        you still have to query sites table to get the root path of a site
        """
        method = method.lower()
        returnCode = 0
        stdinput = ''
        stdoutput =''
        stderror = ''
        #print 'method is ->%s<-' % method
        #print type (method)
        if 'cp' in method:
      #  if method is 'cp':
        #    print "going to cp the file %s" % source
            finalTransportCall = "%s %s %s" % (method, source, destination)
        elif method is 'ln':
#            print "going to ln the file %s" % source
            finalTransportCall = "%s %s %s" % (method, source, destination)
        elif method is 'gridftp':
            print "to be implemented"
            finalTransportCall = ""
        else:
            print "did not find anything to work upon"
            finalTransportCall = ''
        if finalTransportCall != '':
#            print "going to %s the file: %s with the finalTransportCall: %s" %(method,source,finalTransportCall)
            #returnCode = os.system(finalTransportCall)
#            returnCode = subprocess.call(finalTransportCall.split(),stdinput,stdoutput,stderror)
          #  returnCode = subprocess.call(['cp','/home/ankitc/devel/FileMgmt/trunk/python/filemgmt/test.txt','/home/ankitc/test/'],stdinput,stdoutput,stderror)
            #returnCode = subprocess.call(['cp','/home/ankitc/devel/FileMgmt/trunk/python/filemgmt/testingcache.txt','/home/ankitc/test/'])
            "run the cp/ln/grid-ftp command"
            returnStats = self.runCommand(finalTransportCall)
        else:
            print "did not transfer file. we are not ready for that yet"
        
        #if return != '':
            print "there was an error in transporting file error  %s " %(returnStats)
            
        return returnCode
    
    def pre_put (self,list,cache_name):
        "comments go here"
        return
    def get_within_job_wrapper(self,fileList,cache_name,site_name):
        """
        this function will get the files from a cache and put them into the runsite directory. 
        Inputs are an array of files with their complete path to which they need to be moved
        """
        colList = {}#['name','on_target','not_on_target','root','cache_name']
        fileNameDict = {}
        fileStr = ""
        p = re.compile("(\.fits)(\.fz)?")
        for filePath in fileList:
            fileBaseName = os.path.basename(os.path.realpath(filePath))
            fileBaseName = p.sub("",fileBaseName)
            fileStr = fileStr + "'" + fileBaseName + "', "
            fileNameDict[fileBaseName] = filePath 
        "remove the last 2 characters"    
        fileStr = "(" + fileStr[:-2] + ")" 
        
        """get the files which are registered in cache. 
        this means that the files should be present in cache (unless some scientist deletes them by hand ;) )"""
        sqlQueryForFilesInCache = "select path,filename from ops_cache_location where filename in " +  fileStr
#        print "sql to get files" + sqlQueryForFilesInCache
        resultFilesInCache = fileutils.get_queryresult_dictionay(sqlQueryForFilesInCache,self._cur)
        #print "the files in cache are"
        #print resultFilesInCache
        for key in resultFilesInCache:
         #   print "the filename is: " 
            tempFileName = key['filename']
          #  print tempFileName
            key['destination_path']  =   fileNameDict[tempFileName]
        #print "the files in cache AFTER are"
        #print resultFilesInCache
        
        """
        get the details of cache in a dictionary
        """
        sqlGetCacheInfo = "select * from ops_data_defs where type = 'cache' and name = '%s'" % cache_name
        cacheInfoDict = fileutils.get_queryresult_dictionay(sqlGetCacheInfo,self._cur)
#        print " the query sqlGetCacheInfo %s returned %s" % (sqlGetCacheInfo,cacheInfoDict)
        
        """
        get the information about the runsite from the database
        """
        sqlGetSiteInfo = "select * from ops_data_defs where type = 'runsite' and name = '%s'" % site_name
        #print sqlGetSiteInfo
        resultSite = fileutils.get_queryresult_dictionay(sqlGetSiteInfo,self._cur)
        runsite_root = resultSite[0]['root']
#        print "inputs for get_within_job filesDictionary: "
#        print resultFilesInCache
#        print "runsite root: "
#        print runsite_root
#        print "cache table information: "
#        print cacheInfoDict[0]
        self.get_within_job(resultFilesInCache,runsite_root,cacheInfoDict[0])

        return
    
    def get_within_job(self,file_list,runsite_root,cacheInfoDict):
        """
           This will be removed from the path of source file to get the relative path which will be created inside the cache directory
        """
        returnCodeDict = {}
        runsite_root_canonicalized = os.path.realpath(runsite_root)
        #print "the runsite is " + os.path.realpath(runsite_root)
        
        onTargetMethod = cacheInfoDict['on_target']
        returnDict = []
        for filePath in file_list:
            """if the destination filepath is relative, convert it into absolute path
               we do this so that the absolute path can be easily used to copy files from one location to another 
               without confusion
            """
            #print "looping over file 1"
            #print filePath['path']
            if not os.path.isabs(os.path.realpath(filePath['path'])):
                "if the filePath provided is relative, add the runsite root to it to make it an absolute path"
                canonicalizedDestinationFilePath = os.path.realpath(runsite_root_canonicalized) + '/' + os.path.realpath(filePath['destination_path'])
            else:
                " if the path is absolute, leave it alone, only change the path to canonicalized"
                canonicalizedDestinationFilePath = os.path.realpath(filePath['destination_path'])
                
            """create the absolute source path for the file
             this is done by adding the root of cache location to the path given in ops_cache_location filename and adding the filename in the end
            """
            canonicalizedfilePathInCache = os.path.realpath(filePath['path'])
            if not os.path.isabs(canonicalizedfilePathInCache):
#                print "the filepath is not abs: " + canonicalizedfilePathInCache
                canonicalizedfilePathInCache = cacheInfoDict['root'] + '/' + canonicalizedfilePathInCache
                canonicalizedSourcePath = os.path.realpath(cacheInfoDict['root'] + '/' + canonicalizedfilePathInCache + '/' + filePath['filename'])
            else:
#                print "canonicalizedfilePathInCache is abs: " + canonicalizedfilePathInCache
                canonicalizedSourcePath = os.path.realpath(canonicalizedfilePathInCache + '/' + filePath['filename'])
            

            #destination = runsite_root + "/" + os.path.realpath(file).replace(os.path.realpath(resultDict['root']),'')
            #print "the destination filepath is %s" % destination
            """
            call transportFile function and store the results in to a dictionary.
            this dictionary will be used later to update the database
            """
            fileName = os.path.basename(filePath['filename'])

            if self.transportFile(canonicalizedSourcePath,canonicalizedDestinationFilePath,onTargetMethod) is not 0:
                returnDict.append(filePath)
            
        return returnDict
            
        #self.logTransportedFilesInDB(returnCodeDict)
    
    def do_query(query, show=1):
        stmt = dbh.createStatement()
        if show: print query
        rset = stmt.executeQuery(query)

        return  rset
    
    
    def runCommand (self,cmd):
        """runCommand is used to run a system command. input is the command to be run. It returns the stdout and stderr texts."
        stdout and stderr should be empty for a successful command execution"""
        #if verbose: print cmd
        p = subprocess.Popen(cmd, shell=True,stdin= open("/dev/null"), 
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
        (stdouttxt, stderrtxt) = p.communicate()
        stdouttxt = " ".join(stdouttxt.splitlines()) # get rid of newlines
        stderrtxt = " ".join(stderrtxt.splitlines()) # get rid of newlines 
     
        return (stdouttxt, stderrtxt)


#### Embedded simple test
if __name__ ==  '__main__' :
    cache = Cache()
    list = ['/home/ankitc/work/cache_test_runsite/dec021593_11_scamp.fits','/home/ankitc/work/cache_test_runsite/DES2219-4147_g.fits.fz']
    cache.put_within_job_wrapper(list,'destest_cache','destest_site')

    #cache.get_within_job_wrapper(list,'destest_cache','destest_site')
#    print dbh.get_column_names('archive_sites')

