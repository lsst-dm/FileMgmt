import sys
import os
import time


def check_db_duplicates(dbh, filelist, archive):  #including compression
    table = dbh.load_filename_gtt(filelist)
    sql = "select fai.path, art.filename, art.compression,art.id, art.md5sum, art.filesize from desfile art, file_archive_info fai, %s gtt where fai.desfile_id=art.id and fai.archive_name='%s' and gtt.filename=art.filename and coalesce(fai.compression,'x') = coalesce(gtt.compression,'x')" % (
        table, archive)

    curs = dbh.cursor()
    curs.execute(sql)
    results = curs.fetchall()

    if len(results) == len(filelist):
        return {}
    duplicates = {}
    templist = []
    desc = [d[0].lower() for d in curs.description]

    for row in results:
        fdict = dict(list(zip(desc, row)))
        fname = fdict['filename']
        if fdict['compression'] is not None:
            fname += fdict['compression']

        if fname not in templist:
            templist.append(fname)
        else:
            if fname not in duplicates:
                duplicates[fname] = []
            duplicates[fname].append(fdict)
        if "path" in fdict:
            if fdict["path"][-1] == '/':
                fdict['path'] = fdict['path'][:-1]

    return duplicates


def get_paths_by_id(dbh, args):
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
    valid = True

    # check archive is valid archive name (and get archive root)
    sql = "select root from ops_archive where name=%s" % \
          dbh.get_named_bind_string('name')

    curs = dbh.cursor()
    curs.execute(sql, {'name': args.archive})
    rows = curs.fetchall()
    cnt = len(rows)
    if cnt != 1:
        print("Invalid archive name (%s).   Found %s rows in ops_archive" % (args.archive, cnt))
        print("\tAborting")
        sys.exit(1)

    archive_root = rows[0][0]

    if args.pfwid:
        sql = "select archive_path, data_state, operator, reqnum, unitname, attnum from pfw_attempt where id=%s" % \
            (dbh.get_named_bind_string('pfwid'))
        curs.execute(sql, {'pfwid': args.pfwid})
        rows = curs.fetchall()

        relpath = rows[0][0]
        state = rows[0][1]
        operator = rows[0][2]
        args.reqnum = rows[0][3]
        args.unitname = rows[0][4]
        args.attnum = rows[0][5]
        pfwid = int(args.pfwid)

    else:
        ### sanity check relpath
        sql = "select archive_path, data_state, operator, id from pfw_attempt where reqnum=%s and unitname=%s and attnum=%s" % \
            (dbh.get_named_bind_string('reqnum'), dbh.get_named_bind_string('unitname'),
             dbh.get_named_bind_string('attnum'))
        curs.execute(sql, {'reqnum': args.reqnum,
                           'unitname': args.unitname,
                           'attnum': args.attnum})
        rows = curs.fetchall()

        relpath = rows[0][0]
        state = rows[0][1]
        operator = rows[0][2]
        pfwid = rows[0][3]

    if relpath is None:
        print("Path is NULL in database.")
        sys.exit(1)
    archive_path = os.path.join(archive_root, relpath)

    return archive_root, archive_path, relpath, state, operator, pfwid


def get_paths_by_path(dbh, args):
    valid = True

    # check archive is valid archive name (and get archive root)
    sql = "select root from ops_archive where name=%s" % \
          dbh.get_named_bind_string('name')

    curs = dbh.cursor()
    curs.execute(sql, {'name': args.archive})
    rows = curs.fetchall()
    cnt = len(rows)
    if cnt != 1:
        print("Invalid archive name (%s).   Found %s rows in ops_archive" % (args.archive, cnt))
        print("\tAborting")
        sys.exit(1)

    archive_root = rows[0][0]
    ### sanity check relpath
    sql = "select data_state, operator, id from pfw_attempt where archive_path=%s" % (
        dbh.get_named_bind_string('apath'))
    curs.execute(sql, {'apath': args.relpath, })
    rows = curs.fetchall()

    state = rows[0][0]
    operator = rows[0][1]
    pfwid = rows[0][2]
    archive_path = os.path.join(archive_root, args.relpath)

    return archive_root, archive_path, args.relpath, state, operator, pfwid


def get_paths_by_path_compare(dbh, args):
    valid = True

    # check archive is valid archive name (and get archive root)
    sql = "select root from ops_archive where name=%s" % \
          dbh.get_named_bind_string('name')

    curs = dbh.cursor()
    curs.execute(sql, {'name': args.archive})
    rows = curs.fetchall()
    cnt = len(rows)
    if cnt != 1:
        print("Invalid archive name (%s).   Found %s rows in ops_archive" % (args.archive, cnt))
        print("\tAborting")
        sys.exit(1)

    archive_root = rows[0][0]

    archive_path = os.path.join(archive_root, args.relpath)

    return archive_root, archive_path, args.relpath


def get_files_from_db(dbh, relpath, archive, pfwid, filetype=None, debug=False, quick=False):
    """ Query DB to get list of files within that path inside the archive 

        Parameters
        ----------
        dbh : database connection
            The database connection to use

        relpath : str
            The relative path of the directory to gather info for

        archive : str
            The archive name to use

        debug : bool
            Whether or not to report debugging information

        Returns
        -------
        Dictionary containing the file info from the archive (path, name, filesize, md5sum)
    """

    start_time = time.time()
    if debug:
        print("Getting file information from db: BEG")

    if filetype is not None:
        sql = "select fai.path, art.filename, art.compression, art.id, art.md5sum, art.filesize from desfile art, file_archive_info fai where art.pfw_attempt_id=%i and fai.desfile_id=art.id and art.filetype='%s' and fai.archive_name='%s'" % (
            pfwid, filetype, archive)
    else:
        if pfwid is not None:
            if quick:
                sql = "select fai.path, art.filename, art.compression,art.id, art.md5sum, art.filesize from desfile art, file_archive_info fai where art.pfw_attempt_id=%i and fai.desfile_id=art.id and fai.archive_name='%s'" % (
                    pfwid, archive)

            else:
                # when remove filesize from fai, need to change NVL(art.filesize,fai.filesize) as filesize to art.filesize
                sql = "select fai.path, art.filename, art.compression,art.id, art.md5sum, art.filesize from desfile art, file_archive_info fai where (art.pfw_attempt_id=%i or fai.path like '%s%%') and fai.desfile_id=art.id and fai.archive_name='%s'" % (
                    pfwid, relpath, archive)
        else:
            if quick:
                sql = "select fai.path, art.filename, art.compression,art.id, art.md5sum, art.filesize from desfile art, file_archive_info fai where fai.desfile_id=art.id and fai.archive_name='%s'" % (
                    archive)

            else:
                sql = "select fai.path, art.filename, art.compression,art.id, art.md5sum, art.filesize from desfile art, file_archive_info fai where fai.desfile_id=art.id and fai.archive_name='%s' and fai.path like '%s%%'" % (
                    archive, relpath)

    if debug:
        print("\nsql = %s\n" % sql)

    curs = dbh.cursor()
    curs.execute(sql)
    if debug:
        print("executed")
    desc = [d[0].lower() for d in curs.description]

    filelist = []

    files_from_db = {}
    for row in curs:
        fdict = dict(list(zip(desc, row)))
        fname = fdict['filename']
        if fdict['compression'] is not None:
            fname += fdict['compression']
        filelist.append(fname)
        files_from_db[fname] = fdict
        if "path" in fdict:
            if fdict["path"][-1] == '/':
                fdict['path'] = fdict['path'][:-1]
        #    m = re.search("/p(\d\d)",fdict["path"])
        #    if m:
        #        fdict["path"] = fdict["path"][:m.end()]
    duplicates = check_db_duplicates(dbh, filelist, archive)
    end_time = time.time()
    if debug:
        print("Getting file information from db: END (%s secs)" % (end_time - start_time))
    return files_from_db, duplicates
