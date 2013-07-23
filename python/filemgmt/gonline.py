#!/usr/bin/env python


from  datetime import datetime, timedelta
import globusonline.transfer.api_client
#from globusonline.transfer.api_client import Transfer, Delete, APIError, x509_proxy
from globusonline.transfer.api_client import Transfer, x509_proxy
import time
from coreutils.miscutils import *
from filemgmt.filemgmt_defs import *



class DESGlobusOnline():
    def __init__(self, srcinfo, dstinfo, credfile, gouser, proxy_valid_hrs=6):
        self.srcinfo = srcinfo
        self.dstinfo = dstinfo
        clientargs = ['-c', credfile, gouser]
        self.goclient, _ = globusonline.transfer.api_client.create_client_from_args(clientargs)
        self.proxy_valid_hrs = proxy_valid_hrs
        self.credfile = credfile


    def endpoint_activate(self, endpoint):
        _, _, reqs = self.goclient.endpoint_activation_requirements(endpoint, type="delegate_proxy")
        public_key = reqs.get_requirement_value("delegate_proxy", "public_key")
        proxy = x509_proxy.create_proxy_from_file(self.credfile, public_key, self.proxy_valid_hrs)
        reqs.set_requirement_value("delegate_proxy", "proxy_chain", proxy)
        result = self.goclient.endpoint_activate(endpoint, reqs)
        return result


    def start_transfer(self, filelist):
        # activate src endpoint
        src_endpoint = self.srcinfo['endpoint']
        result = self.endpoint_activate(src_endpoint)

        # activate dst endpoint
        dst_endpoint = self.dstinfo['endpoint']
        result = self.endpoint_activate(dst_endpoint)

        ##    Get a submission id:
        code, reason, result = self.goclient.transfer_submission_id()
        self.submission_id = result["value"]
        fwdebug(0, 'GLOBUS_ONLINE_DEBUG', "\tsubmission id = %s" % self.submission_id)

        ##    Create a transfer object:
        #t = Transfer(submission_id, src_endpoint, dst_endpoint, notify_on_succeeded = False,
        #  notify_on_failed = False, notify_on_inactive= False, deadline='2m')
        deadline = datetime.utcnow() + timedelta(minutes=30)
        t = Transfer(self.submission_id, src_endpoint, dst_endpoint, notify_on_succeeded = False,
                     notify_on_failed = False, notify_on_inactive= False, deadline=deadline)
        #print t.as_data()

        # add files to transfer
        for sfile, dfile in filelist.items():
            fwdebug(0, 'GLOBUS_ONLINE_DEBUG', "\tadding to transfer %s = %s" % (sfile, dfile))
            if sfile.endswith('/'):
                t.add_item(sfile, dfile, recursive=True)  # error if true for file
            else:
                t.add_item(sfile, dfile)

        # start transfer
        status, reason, result = self.goclient.transfer(t)
        task_id = result["task_id"]
        fwdebug(0, 'GLOBUS_ONLINE_DEBUG', "\ttask id = %s" % task_id)

        return task_id


    # blocking transfer
    def blocking_transfer(self, filelist):
        task_id = self.start_transfer(filelist)

        # wait for transfer to complete
        ##    Check the progress of the new transfer:
        MAX_TRIES = 600
        status = "ACTIVE"
        failed_status_cnt = 0
        while status == "ACTIVE" and failed_status_cnt < MAX_TRIES:
            fwdebug(0, 'GLOBUS_ONLINE_DEBUG', "Checking transfer task status")
            status, reason, result = self.goclient.task(task_id)
            status = result["status"]
            fwdebug(0, 'GLOBUS_ONLINE_DEBUG', "\tstatus = %s" % result["status"])
            fwdebug(0, 'GLOBUS_ONLINE_DEBUG', "\tfiles = %s" % result["files"])
            fwdebug(0, 'GLOBUS_ONLINE_DEBUG', "\tsubtasks_total = %s" % result["subtasks_total"])
            fwdebug(0, 'GLOBUS_ONLINE_DEBUG', "\tsubtasks_failed = %s" % result["subtasks_failed"])
            fwdebug(0, 'GLOBUS_ONLINE_DEBUG', "\tsubtasks_retrying = %s" % result["subtasks_retrying"])
            fwdebug(0, 'GLOBUS_ONLINE_DEBUG', "\tnice_status_details = %s" % result["nice_status_details"])
            fwdebug(0, 'GLOBUS_ONLINE_DEBUG', "\n\n")

            if status == "ACTIVE":
                if result['subtasks_retrying'] != MAX_TRIES:
                    failed_status_cnt += 1
                if failed_status_cnt < MAX_TRIES:
                    time.sleep(30)


        self.goclient.task_cancel(task_id)
        status, reason, result = self.goclient.task(task_id)
        status = result["status"]

        #print result["status"]
        #print result["files"]
        #print result["subtasks_total"]
        #print result["subtasks_failed"]
        #print result["subtasks_retrying"]
        #print result["nice_status_details"]
        print "task =", result
        print "\n\n"

        status, reason, result = self.goclient.subtask_list(task_id)
        #print status
        #print reason
        print "----------\n\n\n"
        print "subtask_list=",result
        print "\n\n\n"
        transresults = {}
        for x in result['DATA']:
            print "x=",x,"\n"
            subtask_id = x['task_id']
            print x['parent_task_id'], ":", x['task_id']

            substatus, subreason, subresult = self.goclient.subtask_event_list(subtask_id)
            print "subtask events =", subresult

            if x['source_path'] not in transresults:
                transresults[x['source_path']] = {}
            if x['destination_path'] not in transresults[x['source_path']]:
                transresults[x['source_path']][x['destination_path']] = []
            transresults[x['source_path']][x['destination_path']].append(subresult['DATA'])
            print "\n\n"

        print "\n\n\n\nTRANSRESULTS"
        for s in transresults:
            for d in transresults[s]:
                print s, d, transresults[s][d]
        if failed_status_cnt >=  MAX_TRIES:
            print "cancelling task", task_id

        # create filelist of failures
        #   todo
        #if not x['is_error']:
        #    msg = x['description']  # or x['details']
        # assume for now success

        results = {}
        for src, dst in filelist.items(): 
            results[src] = {'dst': dst}
        return results

if __name__ == "__main__":
    pass
