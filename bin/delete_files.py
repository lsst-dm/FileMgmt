#!/usr/bin/env python

""" Delete files from local disk and DB location tracking based upon an archive path or reqnum/unitname/attnum triplet """

import os
import shutil
import sys
import argparse
import filemgmt.disk_utils_local as diskutils
import filemgmt.db_utils_local as dbutils

import despydb.desdbi as desdbi


def parse_cmd_line(argv):
    """ Parse command line """
    parser = argparse.ArgumentParser(description='Delete files from DB and disk based upon path')
    parser.add_argument('--des_services', action='store', help='')
    parser.add_argument('--section', '-s', action='store', help='Must be specified if DES_DB_SECTION is not set in environment')
    parser.add_argument('--archive', action='store', required=True, help='archive_name from file_archive_info table')
    parser.add_argument('--relpath', action='store', help='relative path on disk within archive (no archive root)')
    parser.add_argument('--reqnum', action='store', help='Request number to search for')
    parser.add_argument('--unitname',action='store', help='Unit name to search for')
    parser.add_argument('--attnum', action='store', help='Attempt number to search for')
    parser.add_argument('--ccdnum', action='store')
    parser.add_argument('--filetype', action='store', help='File type to delete')
    parser.add_argument('--band', action='store')
    parser.add_argument('--expnum', action='store')
    parser.add_argument('--nite', action='store')

    args = parser.parse_args(argv)
    return args

def validate_args(dbh, args):
    """ Make sure command line arguments have valid values """

    if args.relpath:
        ### sanity check relpath
        # make sure relpath is not an absolute path
        if args.relpath[0] == '/':
            print "Error: relpath is an absolute path  (%s)." % args.relpath[0]
            print "\tIt should be the portion of the path after the archive root."
            print "\tAborting"
            sys.exit(1)

        archive_root,archive_path,relpath,state,operator,pfwid = dbutils.get_paths_by_path(dbh, args)
    elif args.reqnum and args.unitname and args.attnum:
        archive_root, archive_path, relpath, state, operator, pfwid = dbutils.get_paths_by_id(dbh, args)
    else:
        print "Either relpath or a reqnum/unitname/attnum triplet must be specified."
        sys.exit(1)

    # make sure path is at least 3 directories longer than archive_root
    subdirs = relpath.strip('/').split('/')  # remove any trailing / first
    if len(subdirs) < 3:
        print "Suspect relpath is too high up in archive (deleting too much)."
        print "\tCheck relpath is accurate.   If actually want to delete all that,"
        print "\tcall program on smaller chunks"
        print "\tAborting"
        sys.exit(1)

    # check path is path exists on disk
    path = "%s/%s" % (archive_root, relpath)
    if not os.path.exists(path):
        print "Path does not exist on disk:  %s" % (path)
        print "\tAborting"
        sys.exit(1)

    return archive_root,archive_path,relpath,state,operator,pfwid

def print_files(files_from_disk, comparison_info):
    """ Print both lists of files """

    print "Files in both database and on disk:\n"
    for fn in files_from_disk.keys():
        if fn not in comparison_info['dbonly'] and fn not in comparison_info['diskonly']:
            print "    %s" % (fn)
    print "\n"
    print " Files only found in database:\n"
    if len(comparison_info['dbonly']) == 0:
        print "None\n\n"
    else:
        for fn in comparison_info['dbonly']:
            print "    %s" % (fn)
        print "\n"
    print " Files only found on disk:\n"
    if len(comparison_info['diskonly']) == 0:
        print "None\n\n"
    else:
        for fn in comparison_info['diskonly']:
            print "    %s" % (fn)
        print "\n"

def diff_files(comparison_info):
    """ Print only differences in file lists """
    if len(comparison_info['dbonly']) == len(comparison_info['diskonly']) == 0:
        print " No differneces found\n"
        return
    print " Files only found in database:\n"
    if len(comparison_info['dbonly']) == 0:
        print "None\n\n"
    else:
        for fn in comparison_info['dbonly']:
            print "    %s" % (fn)
        print "\n"
    print " Files only found on disk:\n"
    if len(comparison_info['diskonly']) == 0:
        print "None\n\n"
    else:
        for fn in comparison_info['diskonly']:
            print "    %s" % (fn)
        print "\n"

def del_files_from_disk(path):
    """ Delete files from disk """

    shutil.rmtree(path) #,ignore_errors=True)

def del_files_from_db(dbh, relpath, archive):
    """ delete files from file_archive_info table """
    cur = dbh.cursor()
    cur.execute("delete from file_archive_info where archive_name='%s' and path like '%s%%'" % (archive, relpath))
    dbh.commit()
    #dbh.rollback()

def del_part_files_from_db_by_name(dbh, relpath, archive, delfiles):
    cur = dbh.cursor()
    cur.prepare("delete from file_archive_info where archive_name='%s' and path like '%s%%' and filename=:1" % (archive, relpath))
    print "delete from file_archive_info where archive_name='%s' and path like '%s%%' and filename=:1" % (archive, relpath)
    print delfiles
    cur.executemany(None, delfiles)
    print "ROWS",cur.rowcount
    if cur.rowcount != len(delfiles):
        print "Inconsistency detected: %i rows removed from db and %i files deleted, these should match." % (cur.rowcount, len(delfiles))
    dbh.commit()
    #dbh.rollback()

def del_part_files_from_db(dbh, archive, delfileid):
    df = ""
    for f in delfileid:
        df += str(f) + ","
    df = df[:-1]
    cur = dbh.cursor()
    cur.execute("delete from file_archive_info where archive_name='%s' and desfile_id in (%s)" % (archive, df))
    if len(delfileid) != cur.rowcount:
        print "Inconsistency detected: %i rows removed from db and %i files deleted, these should match." % (cur.rowcount, len(delfileid))
    dbh.commit() 
    #dbh.rollback()

def del_part_files_from_disk(files, archive_root):
    good = []
    for k, v in files.iteritems():
        try:
            os.remove(os.path.join(archive_root, v['path'], k))
            good.append(v['id'])
        except:
            pass
    return good

def main():
    """ Main control """

    args = parse_cmd_line(sys.argv[1:])
    dofiles = False
    if args.filetype is not None:
        dofiles = True
    dbh = desdbi.DesDbi(args.des_services, args.section)
    archive_root, archive_path, relpath, state, operator, pfwid = validate_args(dbh, args)
    files_from_disk = diskutils.get_files_from_disk(relpath, archive_root)
    files_from_db = dbutils.get_files_from_db(dbh, relpath, args.archive, pfwid, args.filetype)
    # if filetype is set then trim down the disk results                                                                                                                                                                                                            
    if args.filetype is not None:
        newfiles = {}
        for fl, val in files_from_db.iteritems():
            if fl in files_from_disk:
                newfiles[fl] = files_from_disk[fl]
        files_from_disk = newfiles

    comparison_info = diskutils.compare_db_disk(files_from_db, files_from_disk, False, True, archive_root=archive_root)

    filesize = 0.0
    for fl, val in files_from_disk.iteritems():
        filesize += val['filesize']
    if filesize >= 1024*1024*1024:
        filesize /= (1024*1024*1024)
        fend = "Gb"
    elif filesize >= 1024*1024:
        filesize /= (1024*1024)
        fend = "Mb"
    else:
        filesize /= 1024
        fend = "kb"

    print "\nOperator = %s" % operator
    print "Path = %s" % archive_path
    print "Archive name = %s" % args.archive
    print "Number of files from disk = %s" % (len(files_from_disk))
    print "Number of files from db   = %s" % (len(files_from_db))
    print "Total file size on disk = %.3f %s" % (filesize,fend)

    if state != "JUNK" and args.filetype is None:
        print "\n  Data state is not JUNK and cannot be deleted."
        sys.exit(1)

    if len(files_from_db) == 0:
        print "\nNo files in database to delete."
        sys.exit(0)
    if len(files_from_disk) == 0:
        print "\nNo files on disk to delete."
        sys.exit(0)

    shdelchar = 'x'
    while shdelchar != 'n' and shdelchar != 'y':
        print ""

        should_delete = raw_input("Do you wish to continue with deletion [yes/no/diff/print]?  ")
        shdelchar = should_delete[0].lower()

        if shdelchar == 'p' or shdelchar ==  'print':
            print_files(files_from_disk, comparison_info)
        elif shdelchar == 'd' or shdelchar == 'diff':
            diff_files(comparison_info)
        elif shdelchar == 'y' or shdelchar == 'yes':
            if dofiles:
                good = del_part_files_from_disk(files_from_db, archive_root)
                if len(good) != len(files_from_db):
                    del_part_files_from_db(dbh, args.archive, good)
                else:
                    #del_files_from_db(dbh, args.relpath, args.archive, len(files.from_db))####NEED TO ADD FILETYPE
                    del_part_files_from_db(dbh, args.archive, good)                    
            else:
                try:
                    del_files_from_disk(archive_path)
                except Exception as e:
                    print "Error encountered when deleting files: ",str(e)
                    print "Aborting"
                    return
                ffd = {}
                for (dirpath, dirnames, filenames) in os.walk(os.path.join(archive_root,relpath)):
                    for filename in filenames:
                        ffd[filename] = dirpath
                if len(ffd) > 0:
                    delfiles = []
                    for fn, val in files_from_disk.iteritems():
                        if fn not in ffd:
                            delfiles.append((fn))
                    del_part_files_from_db_by_name(dbh,relpath,args.archive,delfiles)
                else:
                    del_files_from_db(dbh, relpath, args.archive)
        elif shdelchar == 'n' or shdelchar == 'no':
            print "Aborting."
        else:
            print "Unknown input (%s).   Ignoring" % shdelchar

if __name__ == "__main__":
    main()
