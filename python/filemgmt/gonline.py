#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.


from  datetime import datetime, timedelta
import globusonline.transfer.api_client
#from globusonline.transfer.api_client import Transfer, Delete, APIError, x509_proxy
from globusonline.transfer.api_client import Transfer, x509_proxy
import time
from coreutils.miscutils import *
from filemgmt.filemgmt_defs import *
import sys
import copy



class DESGlobusOnline():
    def __init__(self, srcinfo, dstinfo, credfile, gouser, proxy_valid_hrs=6):
        self.srcinfo = srcinfo
        self.dstinfo = dstinfo
        clientargs = ['-c', credfile, gouser]
        try:
            self.goclient, _ = globusonline.transfer.api_client.create_client_from_args(clientargs)
        except:
            (type, value, traceback) = sys.exc_info()
            print "Error when trying to create globusonline client"
            print "\t%s: %s" % (type, value)
            print "\tTypically means problem with operator proxy.  Check that there is a valid proxy using grid-proxy-info"
            sys.exit(FM_EXIT_FAILURE)
            
        self.proxy_valid_hrs = proxy_valid_hrs
        self.credfile = credfile


    def endpoint_activate(self, endpoint):
        _, _, reqs = self.goclient.endpoint_activation_requirements(endpoint, type="delegate_proxy")
        public_key = reqs.get_requirement_value("delegate_proxy", "public_key")
        proxy = x509_proxy.create_proxy_from_file(self.credfile, public_key, self.proxy_valid_hrs)
        reqs.set_requirement_value("delegate_proxy", "proxy_chain", proxy)
        result = self.goclient.endpoint_activate(endpoint, reqs)
        return result

    def makedirs(self, filelist, endpoint):
        # get list of dirs to make
        print "makedirs: filelist=",filelist
        dirlist = get_list_directories(filelist)
        print "makedirs: dirlist=",dirlist
        for path in sorted(dirlist): # should already be sorted, but just in case
            fwdebug(0, 'GLOBUS_ONLINE_DEBUG', 'endpoint=%s, path=%s' % (endpoint,path))
            try:
                result = self.goclient.endpoint_mkdir(endpoint, path)
            except Exception as e:
                if 'already exists' not in str(e):
                    raise
                else:
                    fwdebug(2, 'GLOBUS_ONLINE_DEBUG', 'already exists endpoint=%s, path=%s' % (endpoint,path))


                

    def start_transfer(self, filelist):
        # activate src endpoint
        src_endpoint = self.srcinfo['endpoint']
        result = self.endpoint_activate(src_endpoint)

        # activate dst endpoint
        dst_endpoint = self.dstinfo['endpoint']
        result = self.endpoint_activate(dst_endpoint)


        # create dst directories
        self.makedirs([finfo['dst'] for finfo in filelist.values()], dst_endpoint)

        ##    Get a submission id:
        code, reason, result = self.goclient.transfer_submission_id()
        self.submission_id = result["value"]
        fwdebug(1, 'GLOBUS_ONLINE_DEBUG', "\tsubmission id = %s" % self.submission_id)

        ##    Create a transfer object:
        #t = Transfer(submission_id, src_endpoint, dst_endpoint, notify_on_succeeded = False,
        #  notify_on_failed = False, notify_on_inactive= False, deadline='2m')
        deadline = datetime.utcnow() + timedelta(minutes=30)
        t = Transfer(self.submission_id, src_endpoint, dst_endpoint, notify_on_succeeded = False,
                     notify_on_failed = False, notify_on_inactive= False, deadline=deadline)
        #print t.as_data()

        # add files to transfer
        for fname, finfo in filelist.items():
            sfile = finfo['src']
            dfile = finfo['dst']
            fwdebug(2, 'GLOBUS_ONLINE_DEBUG', "\tadding to transfer %s = %s" % (sfile, dfile))
            if sfile.endswith('/'):
                t.add_item(sfile, dfile, recursive=True)  # error if true for file
            else:
                t.add_item(sfile, dfile)

        # start transfer
        status, reason, result = self.goclient.transfer(t)
        task_id = result["task_id"]
        fwdebug(1, 'GLOBUS_ONLINE_DEBUG', "\ttask id = %s" % task_id)

        return task_id


    # blocking transfer
    def blocking_transfer(self, filelist):
        task_id = self.start_transfer(filelist)
        fwdebug(0, 'GLOBUS_ONLINE_DEBUG', "\ttask_id = %s" % task_id)

        # wait for transfer to complete
        ##    Check the progress of the new transfer:
        MAX_NUM_CHKS = 600
        MAX_NUM_RETRY = 5
        CHK_INTERVAL_SECS = 30



        status = "ACTIVE"
        chk_cnt = 0
        retry_cnt = 0
        errstrs = {}
        while status == "ACTIVE" and chk_cnt < MAX_NUM_CHKS and retry_cnt < MAX_NUM_RETRY:
            fwdebug(1, 'GLOBUS_ONLINE_DEBUG', "Checking transfer task status")
            status, reason, result = self.goclient.task(task_id)
            status = result["status"]
            fwdebug(1, 'GLOBUS_ONLINE_DEBUG', "\tstatus = %s" % result["status"])
            fwdebug(1, 'GLOBUS_ONLINE_DEBUG', "\tfiles = %s" % result["files"])
            fwdebug(1, 'GLOBUS_ONLINE_DEBUG', "\tsubtasks_total = %s" % result["subtasks_total"])
            fwdebug(1, 'GLOBUS_ONLINE_DEBUG', "\tsubtasks_failed = %s" % result["subtasks_failed"])
            fwdebug(1, 'GLOBUS_ONLINE_DEBUG', "\tsubtasks_retrying = %s" % result["subtasks_retrying"])
            fwdebug(1, 'GLOBUS_ONLINE_DEBUG', "\tnice_status_details = %s" % result["nice_status_details"])

            if status == "ACTIVE":
                chk_cnt += 1

                # cannot call task_successful_transfers on task that is still active
                if result["nice_status_details"] is not None and result["nice_status_details"].startswith("Error"):
                    # only print error message once
                    if result["nice_status_details"] not in errstrs: 
                        print result["nice_status_details"]
                        errstrs[result["nice_status_details"]] = True

                    if result['subtasks_retrying'] != 0:
                        retry_cnt += 1
                    else:
                        fwdebug(0, 'GLOBUS_ONLINE_DEBUG', "\tstatus = %s" % result["status"])
                        fwdebug(0, 'GLOBUS_ONLINE_DEBUG', "\tfiles = %s" % result["files"])
                        fwdebug(0, 'GLOBUS_ONLINE_DEBUG', "\tsubtasks_total = %s" % result["subtasks_total"])
                        fwdebug(0, 'GLOBUS_ONLINE_DEBUG', "\tsubtasks_failed = %s" % result["subtasks_failed"])
                        fwdebug(0, 'GLOBUS_ONLINE_DEBUG', "\tsubtasks_retrying = %s" % result["subtasks_retrying"])
                        fwdebug(0, 'GLOBUS_ONLINE_DEBUG', "\tnice_status_details = %s" % result["nice_status_details"])
                        fwdie("Error while transfering files", FM_EXIT_FAILURE)

                if chk_cnt < MAX_NUM_CHKS and retry_cnt < MAX_NUM_RETRY:
                    time.sleep(CHK_INTERVAL_SECS)


        self.goclient.task_cancel(task_id)
        #status, reason, result = self.goclient.task(task_id)
        #status = result["status"]

        #print result["status"]
        #print result["files"]
        #print result["subtasks_total"]
        #print result["subtasks_failed"]
        #print result["subtasks_retrying"]
        #print result["nice_status_details"]
        #print "task =", result
        #print "\n\n"

        status, reason, successes = self.goclient.task_successful_transfers(task_id)
        print status
        print reason
        print "----------\n\n\n"
        print "subtask_list=",result
        print "\n\n\n"

        transresults = copy.deepcopy(filelist)
        if len(successes['DATA']) != len(filelist):
            for fname,finfo in transresults.items():
                transresults[fname]['err'] = 'problems transferring file'
        return transresults

if __name__ == "__main__":
    pass
