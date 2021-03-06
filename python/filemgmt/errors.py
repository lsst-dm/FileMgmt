"""Define some exceptions.
"""



class MetadataConfigError (Exception):
    """Represent an error in the METADATA and/or FILETYPE tables.
    """

    def __init__(self, msg):
        Exception.__init__(self, msg)


class DuplicateDBFiletypeError (MetadataConfigError):
    """Duplicate filetype in the FILETYPE table in the database.
    """

    def __init__(self, msg=None):
        if not msg:
            msg = 'Filetypes differing only by case exist in filetype table.'
        MetadataConfigError.__init__(self, msg)


class DuplicateDBHeaderError (MetadataConfigError):
    """Duplicate header in the METADATA table in the database.
    """

    def __init__(self, msg=None):
        if not msg:
            msg = 'Header names differing only by case exist in metadata table.'
        MetadataConfigError.__init__(self, msg)


class IdMetadataHeaderError (MetadataConfigError):
    """Disallowed id header found in METADATA table in the database.
    """

    def __init__(self, msg=None):
        if not msg:
            msg = 'Id header found in metadata table.'
        MetadataConfigError.__init__(self, msg)


class FileMetadataIngestError (Exception):
    """Represent an error in the file metadata ingest routines.
    """

    def __init__(self, msg):
        Exception.__init__(self, msg)


class RequiredMetadataMissingError (FileMetadataIngestError):
    """A required file metadata element was not found in the dataset to ingest.
    """

    def __init__(self, msg=None):
        if not msg:
            msg = 'Required metadata element was not found.'
        FileMetadataIngestError.__init__(self, msg)


class DBMetadataNotFoundError (FileMetadataIngestError):
    """There is an unknown filetype in the file submitted for ingest.
    """

    def __init__(self, msg=None):
        if not msg:
            msg = 'The file\'s filetype was not found in the database.'
        FileMetadataIngestError.__init__(self, msg)
