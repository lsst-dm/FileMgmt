""" Compare files from local disk and DB location tracking based upon an archive path """
import os
import time
from sets import Set

import despydmdb.desdmdbi as desdmdbi
import disk_utils_local as diskutils
import db_utils_local as dbutils

class Args(object):
    def __init__(self, **kw):
        for item, val in kw.iteritems():
            setattr(self, item, val)

def validate_args(dbh, args):
    """ Make sure command line arguments have valid values 

        Parameters
        ----------
        dbh : database connection
            connection to use for checking the database related argumetns

        args : dict
            dictionary containing the command line arguemtns

        Returns
        -------
        string containing the archive root

    """
    if args.relpath is not None:
        archive_root, archive_path, relpath = dbutils.get_paths_by_path_compare(dbh, args)
        operator = None
        pfwid = None
    elif (args.reqnum and args.unitname and args.attnum) or args.pfwid:
        archive_root, archive_path, relpath, state, operator, pfwid = dbutils.get_paths_by_id(dbh, args)
    else:
        raise Exception("Either relpath, pfwid, or a reqnum/unitname/attnum triplet must be specified.")
    # check path exists on disk
    if not os.path.exists(archive_path):
        print "Warning: Path does not exist on disk:  %s" % (archive_path)
    if args.verbose:
        args.silent = False
    return archive_root, archive_path, relpath, operator, pfwid


def print_all_files(comparison_info, files_from_db, files_from_disk):
    """ Print both lists of files 

        Parameters
        ----------
        comparison_info : dict
            Dictionary containing the results of the comparisons

        files_from_db : dict
            Dicitonary containing the file info from the database

        files_from_disk : dict
            Dictionary containing the file info from disk

        Returns
        -------
        None

    """

    print "db path/name (filesize, md5sum)   F   disk path/name (filesize, md5sum)"
    allfiles = Set(files_from_db).union(Set(files_from_disk))
    fdisk_str = ""
    for fname in allfiles:
        if fname in files_from_db:
            finfo = files_from_db[fname]
            fullname = "%s/%s" % (finfo['path'], fname)
            filesize = None
            if 'filesize' in finfo:
                filesize = finfo['filesize']
            md5sum = None
            if 'md5sum' in finfo:
                md5sum = finfo['md5sum']

            fdb_str = "%s (%s, %s)" % (fullname, filesize, md5sum)
        else:
            fdb_str = ""

        if fname in files_from_disk:
            finfo = files_from_disk[fname]
            fullname = "%s/%s" % (finfo['relpath'], fname)
            filesize = None
            if 'filesize' in finfo:
                filesize = finfo['filesize']
            md5sum = None
            if 'md5sum' in finfo:
                md5sum = finfo['md5sum']

            fdisk_str = "%s (%s, %s)" % (fullname, filesize, md5sum)
        else:
            fdisk_str = ""

        cmp = 'X'
        if fname in comparison_info['equal']: 
            cmp = '='
            
        print "%-140s %s %-140s" % (fdb_str, cmp, fdisk_str)

def diff_files(comparison_info, files_from_db, files_from_disk, check_md5sum, check_filesize, duplicates, db_duplicates):
    """ Print only differences in file lists 

        Parameters
        ----------
        comparison_info : dict
            Dictionary containing the comparisons of disk and db for each file

        file_from_db : dict
            Dicitonary containing the file info from the database

        files_from_disk : dict
            Dictionary containing the file info from disk

        check_md5sum : bool
            Whether or not to report the md5sum comparison

        check_filesize : bool
            Whether or not to report the filesize comparison


        Returns
        -------
        None
    """
    pdup = []

    if len(comparison_info['dbonly']) > 0:
        print "Files only found in the database --------- "
        for fname in sorted(comparison_info['dbonly']):
            fdb = files_from_db[fname]
            print "\t%s/%s" % (fdb['path'], fname)
    

    if len(comparison_info['diskonly']) > 0:
        print "\nFiles only found on disk --------- "
        for fname in sorted(comparison_info['diskonly']):
            addon = ""
            if fname in duplicates:
                addon = "  *"
            fdisk = files_from_disk[fname]
            print "\t%s/%s%s" % (fdisk['relpath'], fname, addon)
        if len(comparison_info['pathdup']) > 0:
            print "\n The following files had multiple paths on disk (path  filesize):"
            listing = {}
            for fname in comparison_info['pathdup']:
                pdup.append(fname)
                listing[comparison_info['pathdup']['relpath']] = comparison_info['pathdup']['filesize']
            first = True
            for pth in sorted(listing):
                start = " "
                if first:
                    start = "*"
                    first = False
                addon = ""
                if fname in files_from_db and files_from_db[fname]['path'] == pth:
                    addon ="  (DB Match)"
                print "      %s %s/%s   %i%s" % (start,pth,d, listing[pth],addon)

    if len(comparison_info['path']) > 0:
        print "\nPath mismatch (file name, db path, disk path) --------- "
        for fname in sorted(comparison_info['path']):
            addon = ""
            if fname in duplicates:
                addon = " *"
            fdb = files_from_db[fname]
            fdisk = files_from_disk[fname]
            print "\t%s\t%s\t%s%s" % (fname, fdb['path'],fdisk['relpath'], addon)
        if len(comparison_info['duplicates']) > 0:
            print "  The following files have multiple disk paths on disk (path  filesize):"
            for fname in comparison_info['duplicates']:
                pdup.append(fname)
                listing[comparison_info['duplicates']['relpath']] = comparison_info['duplicates']['filesize']
            first = True
            for pth in sorted(listing):
                start = " "
                if first:
                    start = "*"
                    first = False
                addon = ""
                if fname in files_from_db and files_from_db[fname]['path'] == pth:
                    addon ="  (DB Match)"
                print "      %s %s/%s   %i%s" % (start,pth,d, listing[pth],addon)

    if len(comparison_info['filesize']) > 0:
        print "\nFilesize mismatch (File name, size in DB, size on disk) --------- "
        for fname in sorted(comparison_info['filesize']):
            fdb = files_from_db[fname]
            fdisk = files_from_disk[fname]
            print "\t%s %s %s" % (fname, fdb['filesize'], fdisk['filesize'])

    if 'md5sum' in comparison_info and len(comparison_info['md5sum']) > 0:
        print "\nmd5sum mismatch (File name, sum in DB, sum on disk) --------- "
        for fname in sorted(comparison_info['md5sum']):
            fdb = files_from_db[fname]
            fdisk = files_from_disk[fname]
            print "\t%s %s %s" % (fname, fdb['md5sum'], fdisk['md5sum'])

    if len(duplicates) > len(pdup):
        print "\nThe following files have multiple disk paths on disk (path  filesize):"
    for d in sorted(duplicates):
        if d not in pdup:
            listing = {}
            for f in duplicates[d]:
                listing[f['relpath']] = f['filesize']
            first = True
            for pth in sorted(listing):
                start = " "
                if first:
                    start = "*"
                    first = False
                addon = ""
                if d in files_from_db and files_from_db[d]['path'] == pth:
                    addon ="  (DB Match)"
                print "      %s %s/%s   %i%s" % (start,pth,d, listing[pth],addon)

    if len(db_duplicates) > 0:
        print "\nThe following files have multiple entries in the database (path  filesize):"
    for d in sorted(db_duplicates):
        listing = {}
        for f in db_duplicates[d]:
            listing[f['relpath']] = f['filesize']
        first = True
        for pth in sorted(listing):
            start = " "
            if first:
                start = "*"
                first = False
            addon = ""
            if d in files_from_disk and files_from_disk[d]['path'] == pth:
                addon ="  (Disk Match)"
            print "      %s %s/%s   %i%s" % (start,pth,d, listing[pth],addon)

def run_compare(args):
    if args.dbh is None:
        dbh = desdmdbi.DesDmDbi(args.des_services, args.section)
    else:
        dbh = args.dbh
    if args.date_range:
        curs = dbh.cursor()
        dates = args.date_range.split(',')
        if len(dates) == 1:
            dr = "submittime>=TO_DATE('%s 00:00:01', 'YYYY-MM-DD HH24:MI:SS') and submittime<=TO_DATE('%s 23:59:59', 'YYYY-MM-DD HH24:MI:SS')" % (dates[0], dates[0])
        else:
            dr = "submittime>=TO_DATE('%s 00:00:01', 'YYYY-MM-DD HH24:MI:SS') and submittime<=TO_DATE('%s 23:59:59', 'YYYY-MM-DD HH24:MI:SS')" % (dates[0], dates[1])
        sql = "select id from pfw_attempt where %s" % dr
        if args.pipeline:
            sql += " and subpipeprod='%s'" % args.pipeline
        sql += " order by id"
        curs.execute(sql)
        res = curs.fetchall()
        if not args.silent:
            print "Found %i pfw_attempt_id's for given date range" % (len(res))
        count = 0
        for pdwi in res:
            args.pfwid = pdwi[0]
            count += do_compare(dbh, args)
        return count
    return do_compare(dbh, args)

def do_compare(dbh, args):
    """ Main control """
    archive_root, archive_path, relpath, operator, pfwid = validate_args(dbh, args)

    #print archive_root
    if args.debug:
        print "From DB"
    files_from_db, db_duplicates = dbutils.get_files_from_db(dbh, relpath, args.archive, pfwid, None, debug=args.debug, quick=args.quick)
    if args.debug:
        print "From disk"
    files_from_disk, duplicates = diskutils.get_files_from_disk(relpath, archive_root, args.md5sum, args.debug)
    if args.debug:
        print "Compare"
    comparison_info = diskutils.compare_db_disk(files_from_db, files_from_disk, duplicates, args.md5sum, args.filesize, args.debug,archive_root)

    if not args.script and not args.silent:
        print "\nPath = %s" % (archive_path)
        print "Archive name = %s" % args.archive
        addon = ""
        dbaddon = ""
        if len(duplicates) > 0:
            addon += "(%i are distinct)" % len(files_from_disk)
        if len(db_duplicates) > 0:
            dbaddon += "(%i are distinct)" % len(files_from_db)
        print "Number of files from db   = %i   %s" % (len(files_from_db) + len(db_duplicates), dbaddon)
        print "Number of files from disk = %i   %s" % (len(files_from_disk) + len(duplicates), addon)
        if len(duplicates) > 0:
            print "Files with multiple paths on disk  = %i" % len(duplicates)
        # print summary of comparison
        print "Comparison Summary"
    
        print "\tEqual:\t%i" % len(comparison_info['equal'])
        print "\tDB only:\t%i" % len(comparison_info['dbonly'])
        print "\tDisk only:\t%i" % len(comparison_info['diskonly'])
        print "\tMismatched paths:\t%i" % len(comparison_info['path'])
        print "\tMismatched filesize:\t%i" % len(comparison_info['filesize'])
        if 'md5sum' in comparison_info:
            print "\tMismatched md5sum:\t%i" % len(comparison_info['md5sum'])
        print ""

        if args.debug:
            print_all_files(comparison_info, files_from_db, files_from_disk)
        elif args.verbose:
            diff_files(comparison_info, files_from_db, files_from_disk, args.md5sum, args.filesize, duplicates, db_duplicates)
        if len(comparison_info['dbonly']) == len(comparison_info['diskonly']) == len(comparison_info['path']) == len(comparison_info['filesize']) == 0:
            if 'md5sum' in comparison_info:
                if len(comparison_info['md5sum']) != 0:
                    print "%s  ERROR" % loc
                    return 1
            return 0
        return 1

    else:
        if args.pfwid is not None:
            loc = "%s" % (args.pfwid)
        elif args.relpath is None:
            loc = "%s  %s  %s" % (args.reqnum, args.unitname, args.attnum)
        else:
            loc = args.relpath
        if len(comparison_info['dbonly']) == len(comparison_info['diskonly']) == len(comparison_info['path']) == len(comparison_info['filesize']) == 0:
            if 'md5sum' in comparison_info:
                if len(comparison_info['md5sum']) != 0:
                    if not args.silent:
                        print "%s  ERROR" % loc
                    return 1
            if not args.silent:
                print "%s  OK" % loc
            return 0
        if not args.silent:
            print "%s  ERROR" % loc
        return 1

def compare(dbh=None, des_services=None, section=None, archive='desar2home', reqnum=None, unitname=None, attnum=None, relpath=None,
            pfwid=None, date_range=None, pipeline=None, filesize=True, md5sum=False, quick=False, debug=False, script=False, verbose=False, silent=True):
    return run_compare(Args(**locals()))
