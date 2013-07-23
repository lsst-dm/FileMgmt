#!/usr/bin/env python

# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.


import sys
import re
import coreutils.desdbi as desdbi
from coreutils.miscutils import *



# verbose
#   0 - no printing 
#   1 - table + number of rows deleted
#   2 - sql
#   3 - more detailed debugging info
def delete_db_run(dbh, unitname, reqnum, attnum, verbose=0):
    
    files2del = [] # all filenames that are being deleted
    del_by_table = {'genfile':[]}  # make the genfile entry to shorten log/wcl code

    sql = "select metadata_table, wgb.filename from wgb, ops_filetype where wgb.unitname='%s' and wgb.reqnum='%s' and wgb.attnum='%s' and wgb.filetype=ops_filetype.filetype" % (unitname, reqnum, attnum)
    if verbose >= 2: print sql
    curs = dbh.cursor()
    curs.execute(sql)
    for line in curs:
        if line[0].lower() not in del_by_table:
            del_by_table[line[0].lower()] = []
        del_by_table[line[0].lower()].append(line[1]) 
        files2del.append(line[1])
    
    
    sql = "select inputwcl, outputwcl, log from pfw_wrapper where unitname='%s' and reqnum='%s' and attnum='%s'" % (unitname, reqnum, attnum)
    if verbose >= 2: print sql
    curs = dbh.cursor()
    curs.execute(sql)
    for line in curs:
    #    print line
        if line[0] is not None:
            del_by_table['genfile'].append(line[0])
            files2del.append(line[0])
        if line[1] is not None:
            del_by_table['genfile'].append(line[1])
            files2del.append(line[1])
        if line[2] is not None:
            del_by_table['genfile'].append(line[2])
            files2del.append(line[2])
    

    if verbose > 3:
        pretty_print_dict(del_by_table, None, True, 4)
        print "\n\n\n"
    
    sql = "select id from pfw_exec where unitname='%s' and reqnum='%s' and attnum='%s'" % (unitname, reqnum, attnum) 
    curs.execute(sql)
    execids = [str(line[0]) for line in curs]  # save as str so can easily do join
    if verbose >= 3: print execids

    sql = "select id from opm_artifact where name in ('%s')" % "','".join(files2del)
    if verbose >= 2: print sql
    curs.execute(sql)
    artids = [str(line[0]) for line in curs]  # save as str so can easily do join
    if verbose >= 3: print artids

    sql = "select id from pfw_wrapper where unitname='%s' and reqnum='%s' and attnum='%s'" % (unitname, reqnum, attnum) 
    if verbose >= 2: print sql
    curs.execute(sql)
    wrapids = [str(line[0]) for line in curs]  # save as str so can easily do join
    if verbose >= 3: print wrapids
    


    # deletions

    if verbose >= 1: print "opm_was_generated_by"
    sql = "delete from opm_was_generated_by where opm_process_id in ('%s')" % "','".join(execids)
    if verbose >= 2: print sql
    curs.execute(sql)
    if verbose >= 1: print "\tdeleted %s rows" % curs.rowcount
    

    if verbose >= 1: print "opm_used"
    sql = "delete from opm_used where opm_process_id in ('%s')" % "','".join(execids)
    if verbose >= 2: print sql
    curs.execute(sql)
    if verbose >= 1: print "\tdeleted %s rows" % curs.rowcount
    

    if verbose >= 1: print "opm_was_derived_from"
    sql = "delete from opm_was_derived_from where CHILD_OPM_ARTIFACT_ID in ('%s')" % "','".join(artids)
    if verbose >= 2: print sql
    curs.execute(sql)
    if verbose >= 1: print "\tdeleted %s rows" % curs.rowcount



    if verbose >= 1: print "pfw_exec"
    sql = "delete from pfw_exec where unitname='%s' and reqnum='%s' and attnum='%s'" % (unitname, reqnum, attnum) 
    if verbose >= 2: print sql
    curs.execute(sql)
    if verbose >= 1: print "\tdeleted %s rows" % curs.rowcount


    if verbose >= 1: print "opm_process"
    sql = "delete from opm_process where id in ('%s')" % "','".join(execids)
    if verbose >= 2: print sql
    curs.execute(sql)
    if verbose >= 1: print "\tdeleted %s rows" % curs.rowcount
    
    
    if verbose >= 1: print "qc_processed_value"
    sql = "delete from qc_processed_value where PFW_WRAPPER_ID in ('%s')" % "','".join(wrapids) 
    if verbose >= 2: print sql
    curs.execute(sql)
    if verbose >= 1: print "\tdeleted %s rows" % curs.rowcount
    
    
    if verbose >= 1: print "qc_processed_message"
    sql = "delete from qc_processed_message where PFW_WRAPPER_ID in ('%s')" % "','".join(wrapids) 
    if verbose >= 2: print sql
    curs.execute(sql)
    if verbose >= 1: print "\tdeleted %s rows" % curs.rowcount
    
            
    if verbose >= 1: print "pfw_wrapper"
    sql = "delete from pfw_wrapper where unitname='%s' and reqnum='%s' and attnum='%s'" % (unitname, reqnum, attnum) 
    if verbose >= 2: print sql
    curs.execute(sql)
    if verbose >= 1: print "\tdeleted %s rows" % curs.rowcount
    
    
    if verbose >= 1: print "pfw_job"
    sql = "delete from pfw_job where unitname='%s' and reqnum='%s' and attnum='%s'" % (unitname, reqnum, attnum) 
    if verbose >= 2: print sql
    curs.execute(sql)
    if verbose >= 1: print "\tdeleted %s rows" % curs.rowcount
    
    
    if verbose >= 1: print "pfw_blktask"
    sql = "delete from pfw_blktask where unitname='%s' and reqnum='%s' and attnum='%s'" % (unitname, reqnum, attnum) 
    if verbose >= 2: print sql
    curs.execute(sql)
    if verbose >= 1: print "\tdeleted %s rows" % curs.rowcount
    
    
    if verbose >= 1: print "pfw_block"
    sql = "delete from pfw_block where unitname='%s' and reqnum='%s' and attnum='%s'" % (unitname, reqnum, attnum) 
    if verbose >= 2: print sql
    curs.execute(sql)
    if verbose >= 1: print "\tdeleted %s rows" % curs.rowcount
    
    
    if verbose >= 1: print "pfw_attempt"
    sql = "delete from pfw_attempt where unitname='%s' and reqnum='%s' and attnum='%s'" % (unitname, reqnum, attnum) 
    if verbose >= 2: print sql
    curs.execute(sql)
    if verbose >= 1: print "\tdeleted %s rows" % curs.rowcount
    
    ## Do we want it to remove pfw_unit and pfw_request if that was the only run for those?
    
    if verbose >= 1: print "objects_current"
    sql = "delete from objects_current where filename in ('%s')" % "','".join(files2del)
    if verbose >= 2: print sql
    curs.execute(sql)
    if verbose >= 1: print "\tdeleted %s rows" % curs.rowcount


    if verbose >= 1: print "file_archive_info"
    sql = "delete from file_archive_info where filename in ('%s')" % "','".join(files2del+del_by_table['genfile'])
    if verbose >= 2: print sql
    curs.execute(sql)
    if verbose >= 1: print "\tdeleted %s rows" % curs.rowcount

    for table,filelist in del_by_table.items():
        if filelist is not None and len(filelist) > 0:
            if verbose >= 1: print table
            sql = "delete from %s where filename in ('%s')" % (table, "','".join(filelist))
            if verbose >= 2: print sql
            curs.execute(sql)
            if verbose >= 1: print "\tdeleted %s rows" % curs.rowcount


#    if verbose >= 1: print "opm_artifact"
#    sql = "delete from opm_artifact where id in ('%s')" % "','".join(artids)
#    if verbose >= 2: print sql
#    print sql
#    curs = dbh.cursor()
#    curs.execute(sql)
#    if verbose >= 1: print "\tdeleted %s rows" % curs.rowcount
    

    
    

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: delete_db_run.py <run>"
        sys.exit(1)
    
    run = sys.argv[1]
    
    m = re.search("([^_]+)_r([^p]+)p([^_]+)", run)
    if m is None:
        print "Error:  cannot parse run", run
        sys.exit(1)
    
    unitname = m.group(1)
    reqnum = m.group(2)
    attnum = m.group(3) 
    
    print "unitname =", unitname
    print "reqnum =", reqnum
    print "attnum =", attnum
    
    dbh = desdbi.DesDbi() 

    try:
        delete_db_run(dbh, unitname, reqnum, attnum, 1)
    except:
        print "Caught exception.  Explicitly rolling back database.  No rows are deleted"
        dbh.rollback()
        raise

    print "Are you sure you want to delete all these rows (Y/N)? ",
    ans = sys.stdin.read(1)
    if ans.lower() == 'y':
        print "Committing the deletions"
        dbh.commit()
    else:
        print "Rolling back database.  No rows are deleted"
        dbh.rollback()
