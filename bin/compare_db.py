#!/usr/bin/env python

"""Compare files from local disk and DB location tracking based upon an
archive path.
"""

import sys
import argparse
import filemgmt.compare_utils as compare


class Print(object):
    """ Class to capture printed output and stdout and reformat it to append
        the wrapper number to the lines

        Parameters
        ----------
        wrapnum : int
            The wrapper number to prepend to the lines
    """

    def __init__(self, wrapnum):
        self.old_stdout = sys.stdout
        self.wrapnum = int(wrapnum)

    def write(self, text):
        """Method to capture, reformat, and write out the requested text

            Parameters
            ----------
            test : str
                The text to reformat
        """
        text = text.rstrip()
        if len(text) == 0:
            return
        text = text.replace("\n", "\n%04d: " % (self.wrapnum))
        self.old_stdout.write('%04d: %s\n' % (self.wrapnum, text))

    def close(self):
        """Method to return stdout to its original handle.
        """
        return self.old_stdout

    def flush(self):
        """Method to force the buffer to flush.
        """
        self.old_stdout.flush()


def parse_cmd_line(argv):
    """Parse command line arguments.

    Parameters
    ----------
    args : command line arguments

    Returns
    -------
    Dictionary continaing the command line arguments
    """
    parser = argparse.ArgumentParser(description='Delete files from DB and disk based upon path')
    parser.add_argument('--des_services', action='store', help='')
    parser.add_argument('--section', '-s', action='store',
                        help='Must be specified if DES_DB_SECTION is not set in environment')
    parser.add_argument('--archive', action='store', default='desar2home',
                        help='archive_name from file_archive_info table')
    parser.add_argument('--reqnum', action='store', help='Request number to search for')
    parser.add_argument('--relpath', action='store',
                        help='relative path on disk within archive (no archive root)')
    parser.add_argument('--unitname', action='store', help='Unit name to search for')
    parser.add_argument('--attnum', action='store', help='Attempt number to search for')
    parser.add_argument('--md5sum', action='store_true', help='compare db vs disk check_md5sums')
    parser.add_argument('--filesize', action='store_true', help='compare db vs disk file sizes')
    parser.add_argument('--verbose', action='store_true', help='print differences between db and disk')
    parser.add_argument('--debug', action='store_true',
                        help='print all files, recommend >= 300 char wide terminal')
    parser.add_argument('--script', action='store_true',
                        help='Print only if there are errors, usefule for running in loops in scripts')
    parser.add_argument('--pfwid', action='store', help='pfw attempt id to search for')
    parser.add_argument('--quick', action='store_true')
    parser.add_argument('--silent', action='store_true')
    parser.add_argument('--date_range', action='store')
    parser.add_argument('--pipeline', action='store')
    parser.add_argument('--dbh', action='store', help='Internal use only.')
    parser.add_argument('--log', action='store')
    args = parser.parse_args(argv)
    if args.script:
        args.verbose = False
    return args


if __name__ == "__main__":
    args = parse_cmd_line(sys.argv[1:])
    if args.log is not None:
        stdp = Print(args.log)
        sys.stdout = stdp
    ret = compare.run_compare(args)
    if args.log is not None:
        sys.stdout.flush()
        sys.stdout = stdp.close()
    sys.exit(ret)
