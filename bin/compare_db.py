#!/usr/bin/env python

""" Compare files from local disk and DB location tracking based upon an archive path """
import os
import sys
import time
from sets import Set
import argparse
import re
#import despydb.desdbi as desdbi
import despydmdb.desdmdbi as desdmdbi
import filemgmt.disk_utils_local as diskutils
import filemgmt.db_utils_local as dbutils


def parse_cmd_line(argv):
    """ Parse command line arguments 

        Parameters
        ----------
        args : command line arguments

        Returns
        -------
        Dictionary continaing the command line arguments
    """
    parser = argparse.ArgumentParser(description='Delete files from DB and disk based upon path')
    parser.add_argument('--des_services', action='store', help='')
    parser.add_argument('--section', '-s', action='store', help='Must be specified if DES_DB_SECTION is not set in environment')
    parser.add_argument('--archive', action='store', default='desar2home',help='archive_name from file_archive_info table')
    parser.add_argument('--reqnum', action='store', help='Request number to search for')
    parser.add_argument('--relpath', action='store', help='relative path on disk within archive (no archive root)')
    parser.add_argument('--unitname',action='store', help='Unit name to search for')
    parser.add_argument('--attnum', action='store', help='Attempt number to search for')
    parser.add_argument('--md5sum', action='store_true', help='compare db vs disk check_md5sums')
    parser.add_argument('--filesize', action='store_true', help='compare db vs disk file sizes')
    parser.add_argument('--verbose', action='store_true', help='print differences between db and disk')
    parser.add_argument('--debug', action='store_true', help='print all files, recommend >= 300 char wide terminal')
    parser.add_argument('--script', action='store_true', help='Print only if there are errors, usefule for running in loops in scripts')
    parser.add_argument('--tag', action='store', help='Tag for checking all runs with this name, recommended to use --script=True also.')
    args = parser.parse_args(argv)
    if args.script:
        args.verbose = False
    return args


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
    elif args.reqnum and args.unitname and args.attnum:
        archive_root, archive_path, relpath, state, operator, pfwid = dbutils.get_paths_by_id(dbh, args)
    else:
        print "Either relpath or a reqnum/unitname/attnum triplet must be specified."
        sys.exit(1)
    # check path exists on disk
    #path = "%s/%s" % (archive_root, args.relpath)
    if not os.path.exists(archive_path):
        print "Warning: Path does not exist on disk:  %s" % (archive_path)

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
        comparision_info : dict
            Dictionary containing the comparisions of disk and db for each file

        file_from_db : dict
            Dicitonary containing the file info from the database

        files_from_disk : dict
            Dictionary containing the file info from disk

        check_md5sum : bool
            Whether or not to report the md5sum comparision

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
            for fname in comparsion_info['pathdup']:
                pdup.append(fname)
                listing[comparsion_info['pathdup']['relpath']] = comparsion_info['pathdup']['filesize']
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
                listing[comparsion_info['duplicates']['relpath']] = comparsion_info['duplicates']['filesize']
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


def process(dbh, args):
    #print archive_root
    files_from_db, db_duplicates = dbutils.get_files_from_db(dbh, relpath, args.archive, pfwid, None)
    files_from_disk, duplicates = diskutils.get_files_from_disk(relpath, archive_root, args.md5sum, args.debug)
    comparison_info = diskutils.compare_db_disk(files_from_db, files_from_disk, duplicates, args.md5sum, args.filesize, args.debug,archive_root)

    if not args.script:
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
    else:
        if args.relpath is None:
            loc = "%s  %s  %s" % (args.reqnum, args.unitname, args.attnum)
        else:
            loc = args.relpath
        if len(comparison_info['dbonly']) == len(comparison_info['diskonly']) == len(comparison_info['path']) == len(comparison_info['filesize']) == 0:
            if 'md5sum' in comparison_info:
                if len(comparison_info['md5sum']) != 0:
                    print "%s  ERROR" % loc
                    return
            print "%s  OK" % loc
            return
        print "%s  ERROR" % loc


def main():
    """ Main control """

    args = parse_cmd_line(sys.argv[1:])

    dbh = desdmdbi.DesDmDbi(args.des_services, args.section)
    archive_root, archive_path, relpath, operator, pfwid = validate_args(dbh, args)
    if args.tag is not None:
        curs = dbh.cursor()
        curs.execute("select reqnum,unitname,attnum from pfw_attempt where id in (select pfw_attempt_id from proctag where tag='%s')" % args.tag)
        data = curs.fetchall()
        if len(data) == 0:
            print "No tag found in proctag with name %s" % args.tag
            return
        for d in data:
            args.reqnum = d[0]
            args.unitname = d[1]
            args.attnum = d[2]
            process(dbh, args)
    else:
        process(dbh, args)


if __name__ == "__main__":
    main()
