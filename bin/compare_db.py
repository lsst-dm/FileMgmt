#!/usr/bin/env python

""" Compare files from local disk and DB location tracking based upon an archive path """
import sys
import argparse
import filemgmt.compare_utils as compare


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
    parser.add_argument('--pfwid', action='store', help='pfw attempt id to search for')
    parser.add_argument('--quick', action='store_true')
    parser.add_argument('--silent', action='store_true')
    parser.add_argument('--date_range', action='store')
    parser.add_argument('--pipeline', action='store')
    args = parser.parse_args(argv)
    if args.script:
        args.verbose = False
    return args

if __name__ == "__main__":
    args = parse_cmd_line(sys.argv[1:])
    sys.exit(compare.run_compare(args))

