#!/usr/bin/env python

# $Id: filecompressor.py 11430 2013-04-12 21:41:16Z tomashek $
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

__version__ = "$Rev: 11430 $"

import os
import sys
import time
import argparse
import despymisc.miscutils as miscutils
import despydmdb.desdmdbi as desdmdbi
from filemgmt import filemgmt_defs as fdefs

class FileCompressor:
    
    _infile_full = None
    _outfile_full = None
    _dbh = None
    _infile_artifact_id = None
    _outfile_artifact_id = None
    _archive_name = None
    _archive_root = None
    _path = None
    _filename = None
    _ext = None
    _compressor = None
    _infile_size = None
    _outfile_size = None
    _dbmode = None
    _retcode = None
    _cleanup = None
    _debugDateFormat = '%Y-%m-%d %H:%M:%S'
    _errmsg = None

    def __init__(self, infile, compressor, dbh=None, dbargs=None, cleanup=False):
        self._compressor = compressor
        self._ext = self._compressor.get_extention()
        self._cleanup = cleanup
        self._dbmode = True if dbh else False
        if self._dbmode:
            self._dbh = dbh
            self._infile_artifact_id = dbargs["artifact_id"]
            self._archive_name = dbargs["archive_name"]
            self._archive_root = dbargs["archive_root"]
            self._path = dbargs["path"]
            self._filename = dbargs["filename"]
            self._infile_size = dbargs["filesize"]
            self._infile_full = '/'.join([self._archive_root,self._path,self._filename])
        else:
            self._infile_full = infile
            self._infile_size = os.path.getsize(infile)
        self._outfile_full = self._infile_full + self._ext

    def printprefix(self):
        return time.strftime(self._debugDateFormat) + " - "

    def execute(self):
        sys.stdout.write(self.printprefix() + "compressing %s...." % self._infile_full)
        sys.stdout.flush()
        self._retcode = self._compressor.execute(self._infile_full)
        if self._retcode == 0:
            print "done.",
            self._outfile_size = os.path.getsize(self._outfile_full)
            print "CR=%.2f:1" % (float(self._infile_size) / self._outfile_size)
        else:
            print "ERROR"
            self._errmsg = self._compressor.get_errmsg()
        return self._retcode

    def getOutfileFullpath(self):
        return self._outfile_full

    def getInfileFullpath(self):
        return self._infile_full

    def getInfileSize(self):
        return self._infile_size

    def getOutfileSize(self):
        return self._outfile_size

    def getErrMsg(self):
        return self._errmsg

    def _createartifact(self):
        sqlstr = "select id from %s where name=:fname and compression=:comp" % fdefs.PROV_ARTIFACT_TABLE
        cur = self._dbh.cursor()
        cur.execute(sqlstr,{'fname':self._filename,'comp':self._ext})
        compartid = None
        for row in cur:
            compartid = row[0]
        if compartid == None:
            compartid = self._dbh.get_seq_next_value(fdefs.PROV_ARTIFACT_TABLE + "_SEQ")
            self._dbh.basic_insert_row(fdefs.PROV_ARTIFACT_TABLE,
                {'id':compartid,'name':self._filename,'compression':self._ext})
        self._outfile_artifact_id = compartid

    def _updatearchive(self):
        if self._cleanup:
            self._dbh.basic_update_row('file_archive_info',
                {'compression':self._ext,'filesize':self._outfile_size},
                {'archive_name':self._archive_name,'filename':self._filename})
        else:
            self._dbh.basic_insert_row('file_archive_info',
                {'archive_name':self._archive_name,
                 'filename':self._filename,
                 'compression':self._ext,
                 'path':self._path,
                 'filesize':self._outfile_size})

    def _addwgb(self,task_id):
        self._dbh.basic_insert_row(fdefs.PROV_WGB_TABLE,
                {'task_id':task_id,'opm_artifact_id':self._outfile_artifact_id})

    def _addwdf(self):
        self._dbh.basic_insert_row(fdefs.PROV_WDF_TABLE,
                {'parent_opm_artifact_id':self._infile_artifact_id,
                 'child_opm_artifact_id':self._outfile_artifact_id})

    def _addused(self,task_id):
        self._dbh.basic_insert_row(fdefs.PROV_USED_TABLE,
                {'task_id':task_id,'opm_artifact_id':self._infile_artifact_id})

    def updatedb(self,task_id,do_commit=True):
        if self._dbh == None:
            raise Exception(self.printprefix() + "Cannot update DB; no dbh given")
        if self._retcode == 0:
            self._createartifact()
            self._updatearchive()
            self._addwgb(task_id)
            self._addwdf()
        self._addused(task_id)
        if do_commit:
            self._dbh.commit()

    def getProvenanceData(self, exec_id, updateMe):
        if updateMe == None:
            updateMe = {fdefs.PROV_USED:{},fdefs.PROV_WGB:{},fdefs.PROV_WDF:[]}
        else:
            if fdefs.PROV_USED not in updateMe:
                updateMe[fdefs.PROV_USED] = {}
            if fdefs.PROV_WGB not in updateMe:
                updateMe[fdefs.PROV_WGB] = {}
            if fdefs.PROV_WDF not in updateMe:
                updateMe[fdefs.PROV_WDF] = []
        
        if exec_id in updateMe[fdefs.PROV_USED]:    
            updateMe[fdefs.PROV_USED][exec_id] += fdefs.PROV_DELIM + self._filename
        else:
            updateMe[fdefs.PROV_USED][exec_id] = self._filename

        if exec_id in updateMe[fdefs.PROV_WGB]:
            updateMe[fdefs.PROV_WGB][exec_id] += fdefs.PROV_DELIM + self._filename + self._ext
        else:
            updateMe[fdefs.PROV_WGB][exec_id] = self._filename + self._ext
        updateMe[fdefs.PROV_WDF].append(
            {fdefs.PROV_PARENTS:self._filename,fdefs.PROV_CHILDREN:self._filename + self._ext})



if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Compress files and update the database')
    parser.add_argument('--file',action='store')
    parser.add_argument('-e','--exeargs',action='store',default="",
        help='arguments to pass through to the compression application')
    parser.add_argument('--cleanup',action='store_true',default=False,
        help='after compression, delete the uncompressed version')
    parser.add_argument('--class',action='store',default='fpackcompworker.FpackCompWorker',
        help='the package.class of the compression application wrapper. Default is "fpackcompworker.FpackCompWorker"')
    parser.add_argument('-r','--reqnum',action='store')
    parser.add_argument('-u','--unitname',action='store',
        help='only valid if a reqnum is also provided')
    parser.add_argument('-a','--attnum',action='store',
        help='only valid if a reqnum and unitname are also provided')
    parser.add_argument('-S','--Schema',action='store',default="")
    parser.add_argument('-s','--section',action='store',
        help='section of desservices file to use when connecting to DB')
    parser.add_argument('-f','--filetypes',nargs='*',action='store')

    args, unknown_args = parser.parse_known_args()
    args = vars(args)

    dbh = desdmdbi.DesDmDbi(section=args['section'])
    #artifacts = getFilesToCompress(args,dbh)
    artifacts = []
    compressor = miscutils.dynamically_load_class(args["class"])(args["cleanup"],args["exeargs"])

    prov = {}
    for rowdict in artifacts:
        rowdict["schema"] = args["Schema"]
        filecompressor = FileCompressor(infile=args["file"], 
                                compressor=compressor, 
                                cleanup=args["cleanup"],
                                dbh=dbh,
                                dbargs=rowdict
                                )
        retcode = filecompressor.execute()
        filecompressor.updatedb(task_id=10,do_commit=False)
        filecompressor.getProvenanceData(10,prov)
    
    print str(prov)
    dbh.rollback()
    dbh.close()
