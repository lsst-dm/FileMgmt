# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

# lower case because appears as wcl section and wcl sections are converted to lowercase
META_HEADERS = 'h'
META_COMPUTE = 'c'
META_WCL = 'w'
META_COPY = 'p'
META_REQUIRED = 'r'
META_OPTIONAL = 'o'

FILETYPE_METADATA = 'filetype_metadata'
FILE_HEADER_INFO = 'file_header'

USE_HOME_ARCHIVE_INPUT = 'use_home_archive_input'
USE_HOME_ARCHIVE_OUTPUT = 'use_home_archive_output'

FM_PREFER_UNCOMPRESSED = [None, '.fz']
FM_PREFER_COMPRESSED = ['.fz', None]
FM_UNCOMPRESSED_ONLY = [None]
FM_COMPRESSED_ONLY = ['.fz']

FM_EXIT_SUCCESS = 0
FM_EXIT_FAILURE = 1
FW_MSG_ERROR = 3
FW_MSG_WARN = 2
FW_MSG_INFO = 1
