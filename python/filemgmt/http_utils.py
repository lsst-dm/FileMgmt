# $Id: disk_utils_local.py 18486 2014-01-29 15:58:12Z mgower $
# $Rev:: 18486                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2014-01-29 09:58:12 #$:  # Date of last commit.

"""
Routines for performing tasks on files available through http.
"""

__version__ = "$Rev: 18486 $"

import os
import sys
import shutil
import subprocess
import re
import traceback
import time

import despyserviceaccess.serviceaccess as serviceaccess
import despymisc.miscutils as miscutils
import filemgmt.filemgmt_defs as fmdefs


def http_code_str(hcode):
    codestr = "Unmapped http_code (%s)" % hcode
    code2str = {'200': 'Success/Ok',
                '201': 'Success/Created',
                '204': 'No content (unknown status)',
                '301': 'Directory already existed',
                '304': 'Not modified',
                '400': 'Bad Request (check command syntax)',
                '401': 'Unauthorized (check username/password)',
                '403': 'Forbidden (check url, check perms)',
                '404': 'Not Found (check url exists and is readable)',
                '405': 'Method not allowed',
                '429': 'Too Many Requests (check transfer throttling)',
                '500': 'Internal Server Error', 
                '501': 'Not implemented/understood',
                '507': 'Insufficient storage (check disk space)'}


    # convert given code to str (converting to int can fail)
    if str(hcode) in code2str:
        codestr = code2str[str(hcode)]
    return codestr

def curl_exitcode_str(errorcode):
    errorcode = int(errorcode)
    errstr = "Unmapped errorcode (%s)" % errorcode
    code2str = {0: "Successful",
                1: "Unsupported protocol. This build of curl has no support for this protocol.",
                2: "Failed to initialize.",
                3: "URL malformed. The syntax was not correct.",
                5: "Couldn't resolve proxy. The given proxy host could not be resolved.",
                6: "Couldn't resolve host. The given remote host was not resolved.",
                7: "Failed to connect to host.",
                8: "FTP weird server reply. The server sent data curl couldn't parse.",
                9: "FTP access denied. The server denied login or denied access to the particular resource or directory you wanted to reach. Most often you tried to change to a directory that doesn't exist on the server.",
                11: "FTP weird PASS reply. Curl couldn't parse the reply sent to the PASS request.",
                13: "FTP weird PASV reply, Curl couldn't parse the reply sent to the PASV request.",
                14: "FTP weird 227 format. Curl couldn't parse the 227-line the server sent.",
                15: "FTP can't get host. Couldn't resolve the host IP we got in the 227-line.",
                17: "FTP couldn't set binary. Couldn't change transfer method to binary.",
                18: "Partial file. Only a part of the file was transferred.",
                19: "FTP couldn't download/access the given file, the RETR (or similar) command failed.",
                21: "FTP quote error. A quote command returned error from the server.",
                22: "HTTP page not retrieved. The requested url was not found or returned another error with the HTTP error code being 400 or above. This return code only appears if -f/--fail is used.",
                23: "Write error. Curl couldn't write data to a local filesystem or similar.",
                25: "FTP couldn't STOR file. The server denied the STOR operation, used for FTP uploading.",
                26: "Read error. Various reading problems.",
                27: "Out of memory. A memory allocation request failed.",
                28: "Operation timeout. The specified time-out period was reached according to the conditions.",
                30: "FTP PORT failed. The PORT command failed. Not all FTP servers support the PORT command, try doing a transfer using PASV instead!",
                31: "FTP couldn't use REST. The REST command failed. This command is used for resumed FTP transfers.",
                33: "HTTP range error. The range 'command' didn't work.",
                34: "HTTP post error. Internal post-request generation error.",
                35: "SSL connect error. The SSL handshaking failed.",
                36: "FTP bad download resume. Couldn't continue an earlier aborted download.",
                37: "FILE couldn't read file. Failed to open the file. Permissions?",
                38: "LDAP cannot bind. LDAP bind operation failed.",
                39: "LDAP search failed.",
                41: "Function not found. A required LDAP function was not found.",
                42: "Aborted by callback. An application told curl to abort the operation.",
                43: "Internal error. A function was called with a bad parameter.",
                45: "Interface error. A specified outgoing interface could not be used.",
                47: "Too many redirects. When following redirects, curl hit the maximum amount.",
                48: "Unknown TELNET option specified.",
                49: "Malformed telnet option.",
                51: "The peer's SSL certificate or SSH MD5 fingerprint was not ok.",
                52: "The server didn't reply anything, which here is considered an error.",
                53: "SSL crypto engine not found.",
                54: "Cannot set SSL crypto engine as default.",
                55: "Failed sending network data.",
                56: "Failure in receiving network data.",
                58: "Problem with the local certificate.",
                59: "Couldn't use specified SSL cipher.",
                60: "Peer certificate cannot be authenticated with known CA certificates.",
                61: "Unrecognized transfer encoding.",
                62: "Invalid LDAP URL.",
                63: "Maximum file size exceeded.",
                64: "Requested FTP SSL level failed.",
                65: "Sending the data requires a rewind that failed.",
                66: "Failed to initialize SSL Engine.",
                67: "The user name, password, or similar was not accepted and curl failed to log in.",
                68: "File not found on TFTP server.",
                69: "Permission problem on TFTP server.",
                70: "Out of disk space on TFTP server.",
                71: "Illegal TFTP operation.",
                72: "Unknown TFTP transfer ID.",
                73: "File already exists (TFTP).",
                74: "No such user (TFTP).",
                75: "Character conversion failed.",
                76: "Character conversion functions required.",
                77: "Problem with reading the SSL CA cert (path? access rights?).",
                78: "The resource referenced in the URL does not exist.",
                79: "An unspecified error occurred during the SSH session.",
                80: "Failed to shut down the SSL connection.",
                82: "Could not load CRL file, missing or wrong format (added in 7.19.0).",
                83: "Issuer check failed (added in 7.19.0)."}

    if errorcode in code2str:
        errstr = code2str[errorcode]
    return errstr

class HttpUtils():
    copyfiles_called = 0

    def __init__(self, des_services, des_http_section):
        """Get password for curl and initialize existing_directories variable.

        >>> C = HttpUtils('test_http_utils/.desservices.ini', 'file-http')
        >>> len(C.curl_password)
        25"""
        try:
            # Parse the .desservices.ini file:
            self.auth_params = serviceaccess.parse(des_services, des_http_section)

            # Create the user/password switch:
            self.curl_password = "-u %s:%s\n"%(self.auth_params['user'], self.auth_params['passwd'])
        except Exception as err:
            miscutils.fwdie("Unable to get curl password (%s)" % err, fmdefs.FM_EXIT_FAILURE)

        self.existing_directories = set()


    def check_url(self, P):
        """See if P is a url.

        >>> C = HttpUtils('test_http_utils/.desservices.ini', 'file-http')
        >>> C.check_url("http://desar2.cosmology.illinois.edu")
        ('http://desar2.cosmology.illinois.edu', True)
        >>> C.check_url("hello")
        ('hello', False)"""
        if re.match("^https?:", P):
            return (P, True)
        return (P, False)

    def verify(self, src, dst, useShell, fsize):
        """ Method to verify if a file was completely transferred

        """
        cmd2 = "curl -s -I -f -K - %s" % (dst)

        if not useShell:
            process = subprocess.Popen(cmd2.split(), shell=False, stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        else:
            process = subprocess.Popen(cmd2, shell=True, stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        curl_stdout = process.communicate(self.curl_password)[0]
        rtemp = re.search('Content-Length: ?(\d+)', curl_stdout)
        rfsize = int(rtemp.group(1))
        if fsize == 0:
            fsize = os.path.getsize(src)
        if fsize == rfsize:
            return True
        return False


    def run_curl_command(self, cmd, isTest=False, useShell=False, curlConsoleOutputFile='curl_stdout.txt',
                         secondsBetweenRetries=30, numTries=5, fsize=0, src=None, dst=None, verify=False):
        """Run curl command with password given on stdin.

        >>> ignore = os.path.isfile('./hello.txt') and os.remove('./hello.txt')
        >>> C = HttpUtils('test_http_utils/.desservices.ini', 'file-http')
        >>> C.run_curl_command("curl -f -S -s -K - -X PUT -w 'http_code: %{http_code}\\n' -T test_http_utils/hello.txt http://desar2.cosmology.illinois.edu/DESTesting/hello.txt", isTest=True, useShell=True)
        >>> C.run_curl_command("curl -f -S -s -K - http://desar2.cosmology.illinois.edu/DESTesting/hello.txt", isTest=True, curlConsoleOutputFile='hello.txt')
        >>> os.path.isfile('./hello.txt')
        True
        >>> os.remove('./hello.txt')
        >>> C.run_curl_command("curl -f -S -s -K - -X DELETE http://desar2.cosmology.illinois.edu/DESTesting/hello.txt", isTest=True)
        >>> try:
        ...   C.run_curl_command("curl -f -S -s -K - -X PUT -w 'http_code: %{http_code}\\n' -T test_http_utils/hello.txt http://desar2.cosmology.illinois.edu//DESTesting/baz2/hello.txt",isTest=True,useShell=True, secondsBetweenRetries=1,numTries=2)
        ... except Exception as err:
        ...   print err
        File operation failed with return code 22, http status 403.
        >>> try:
        ...   C.run_curl_command("curl -f -S -s -K - -w 'http_code: %{http_code}\\n' http://desar2.cosmology.illinois.edu/DESTesting/hello12312317089.txt",isTest=True,curlConsoleOutputFile='hello.txt',useShell=True,secondsBetweenRetries=1,numTries=2)
        ... except Exception as err:
        ...   print err
        File operation failed with return code 22, http status 404.
        """
        assert '-K - ' in cmd    # required to be in command so can pass username/password as stdin
        assert '-o ' not in cmd
        # allow environment variable to override numTries
        if 'HTTP_NUM_TRIES' in os.environ:
            numTries = int(os.environ['HTTP_NUM_TRIES'])

        cmd = re.sub("^curl ", "curl -o %s " % curlConsoleOutputFile, cmd)
        print "CMD",cmd
        process = 0
        exitcode = 0
        httpcode = None
        starttime = time.time()
        success = False
        for x in range(0, numTries):
            if not isTest:
                if x == 0:
                    if miscutils.fwdebug_check(3, "HTTP_UTILS_DEBUG"):
                        miscutils.fwdebug_print("curl command: %s" % cmd)
                else:
                    miscutils.fwdebug_print("Repeating curl command after failure (%d): %s" % (x, cmd))

            if not useShell:
                process = subprocess.Popen(cmd.split(), shell=False, stdin=subprocess.PIPE,
                                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            else:
                process = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE,
                                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

            # passing --user name:password as stdin so password doesn't show up in ps
            curl_stdout = process.communicate(self.curl_password)[0]

            exitcode = process.returncode
            http_match = re.search('http_code: ?(\d+)', curl_stdout)
            httpcode = None
            if http_match:
                httpcode = http_match.group(1)

            if miscutils.fwdebug_check(3, "HTTP_UTILS_DEBUG"):
                miscutils.fwdebug_print("curl command: %s" % cmd)
                miscutils.fwdebug_print("curl exitcode: %s (%s)" % (exitcode, curl_exitcode_str(exitcode)))
                if httpcode is not None:
                    miscutils.fwdebug_print("curl http status: %s (%s)" % (httpcode, http_code_str(httpcode)))
                else:
                    miscutils.fwdebug_print("curl http status: unknown")
                miscutils.fwdebug_print("curl stdout: %s" % curl_stdout.strip())

            if not isTest and x > 0:
                print "output:", curl_stdout

            sys.stdout.flush()
            if exitcode == 0:
                if ((httpcode in ['200', '201', '301'] and verify) or httpcode == '204') and dst is not None:
                    # if we get code 204 check to see if it has been transferred by getting file size
                    # check file size
                    if self.verify(src, dst, useShell, fsize):
                        success = True
                        break

                elif httpcode in ['200', '201', '301'] and not verify:
                    success = True
                    break


            # run some diagnostics
            miscutils.fwdebug_print("*" * 75)
            miscutils.fwdebug_print("CURL FAILURE")
            miscutils.fwdebug_print("curl command: %s" % cmd)
            miscutils.fwdebug_print("curl exitcode: %s (%s)" % (exitcode, curl_exitcode_str(exitcode)))
            if httpcode is not None:
                miscutils.fwdebug_print("curl http status: %s (%s)" % (httpcode, http_code_str(httpcode)))
            else:
                miscutils.fwdebug_print("curl http status: unknown")
            miscutils.fwdebug_print("curl stdout: %s" % curl_stdout.strip())

            if x < numTries-1:    # not the last time in the loop
                miscutils.fwdebug_print("Sleeping %s secs" % secondsBetweenRetries)
                time.sleep(secondsBetweenRetries)
            else:
                print "\nDiagnostics:"
                print "Directory info"
                sys.stdout.flush()
                if miscutils.fwdebug_check(10, "HTTP_UTILS_DEBUG"):
                    stat = subprocess.Popen("pwd; find . -exec ls -ld {} \;", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    curl_stdout = stat.communicate()[0]
                    print curl_stdout
                elif src is not None:
                    stat = subprocess.Popen('pwd; ls -ld %s' % (src), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    curl_stdout = stat.communicate()[0]
                    print curl_stdout
                else:
                    stat = subprocess.Popen('pwd', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    curl_stdout = stat.communicate()[0]
                    print curl_stdout
                    print "Source file is not local"
                print "\nFile system disk space usage"
                sys.stdout.flush()
                stat = subprocess.Popen("df -h .", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                curl_stdout = stat.communicate()[0]
                print curl_stdout

                sys.stdout.flush()

                hostm = re.search(r"https?://([^/:]+)[:/]", cmd)
                if hostm:
                    hname = hostm.group(1)
                    try:   # don't let exception here halt
                        print "Running commands to %s for diagnostics" % hname
                        print "\nPinging %s" % hname
                        sys.stdout.flush()
                        stat = subprocess.Popen("ping -c 4 %s" % hname, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        curl_stdout = stat.communicate()[0]
                        print curl_stdout
                        print "\nRunning traceroute to %s" % hname
                        sys.stdout.flush()
                        stat = subprocess.Popen("traceroute %s" % hname, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        curl_stdout = stat.communicate()[0]
                        print curl_stdout
                    except:   # print exception but continue
                        (type, value, trback) = sys.exc_info()
                        traceback.print_exception(type, value, trback, file=sys.stdout)
                        print "\n\nIgnoring remote diagnostics exception.   Continuing.\n"
                else:
                    print "Couldn't find url in curl cmd:", cmd
                    print "Skipping remote diagnostics.\n"

                print "*" * 75
                sys.stdout.flush()

        if not success:
            if os.path.isfile(curlConsoleOutputFile):
                print "Last 50 lines of curlConsoleOutputFile:"
                with open(curlConsoleOutputFile, 'r') as f:
                    lines = f.readlines()
                    if len(lines) > 50:
                        print '\n'.join(lines[len(lines)-50:])
                    else:
                        print '\n'.join(lines)

            msg = "File copy failed with return code %d (%s), " % (exitcode, curl_exitcode_str(exitcode))
            if httpcode is not None:
                msg += " http status %s (%s)" % (httpcode, http_code_str(httpcode))
            else:
                msg += " http status unknown (%s)" % curl_stdout.strip()

            raise Exception(msg)

        if os.path.isfile('curl_stdout.txt'):   # don't use curlConsoleOutputFile variable here.   That deletes file just copied
            os.remove('curl_stdout.txt')
        if not isTest:
            return time.time()-starttime

    def create_http_intermediate_dirs(self, f):
        """Create all directories that are valid prefixes of the URL *f*.

        >>> C = HttpUtils('test_http_utils/.desservices.ini','file-http')
        >>> C.create_http_intermediate_dirs("http://desar2.cosmology.illinois.edu/DESTesting/bar/testfile_dh.txt")
        >>> C.run_curl_command("curl -K - -f -S -s -X DELETE http://desar2.cosmology.illinois.edu/DESTesting/bar/", isTest=True)
        """
        # Making bar/ sometimes returns a 301 status even if there doesn't seem to be a bar/ in the directory.
        m = re.match("(http://[^/]+)(/.*)", f)
        for x in miscutils.get_list_directories([m.group(2)]):
            if x not in self.existing_directories:
                self.run_curl_command("curl -f -S -s -K - -w 'http_code: %%{http_code}\\n' -X MKCOL %s" % m.group(1)+x, useShell=True)
                self.existing_directories.add(x)

    def copyfiles(self, filelist, tstats, secondsBetweenRetriesC=30, numTriesC=5, verify=False):
        """ Copies files in given src,dst in filelist """
        num_copies_from_archive = 0
        num_copies_to_archive = 0
        total_copy_time_from_archive = 0.0
        total_copy_time_to_archive = 0.0
        status = 0
        try:
            for filename, fdict in filelist.items():
                fsize = 0
                if 'filesize' in fdict and fdict['filesize'] is not None:
                    fsize = fdict['filesize']
                try:
                    (src, isurl_src) = self.check_url(fdict['src'])
                    (dst,isurl_dst) = self.check_url(fdict['dst'])
                    if (isurl_src and isurl_dst) or (not isurl_src and not isurl_dst):
                        miscutils.fwdie("Exactly one of isurl_src and isurl_dst has to be true (values: %s %s %s %s)"
                              % (isurl_src, src, isurl_dst, dst), fmdefs.FM_EXIT_FAILURE)
                    common_switches = "-f -S -s -K - -w \'http_code:%{http_code}\\n\'"
                    copy_time = None

                    # if local file and file doesn't already exist
                    if not isurl_dst and not os.path.exists(dst):
                        if tstats is not None:
                            tstats.stat_beg_file(filename)

                        # make the path
                        path = os.path.dirname(dst)
                        if len(path) > 0 and not os.path.exists(path):
                            miscutils.coremakedirs(path)

                        # getting some non-zero curl exit codes, double check path exists
                        if len(path) > 0 and not os.path.exists(path):
                            raise Exception("Error: path still missing after coremakedirs (%s)" % path)
                        copy_time = self.run_curl_command("curl %s %s" % (common_switches, src), curlConsoleOutputFile=dst,
                                                          useShell=True, secondsBetweenRetries=secondsBetweenRetriesC, numTries=numTriesC,verify=verify)
                        if tstats is not None:
                            tstats.stat_end_file(0, fsize)
                    elif isurl_dst:   # if remote file
                        if tstats is not None:
                            tstats.stat_beg_file(filename)

                        # create remote paths
                        self.create_http_intermediate_dirs(dst)
                        copy_time = self.run_curl_command("curl %s -T %s -X PUT %s" % (common_switches, src, dst), useShell=True,
                                                      secondsBetweenRetries=secondsBetweenRetriesC, numTries=numTriesC, fsize=fsize, src=src, dst=dst, verify=verify)

                        if tstats is not None:
                            tstats.stat_end_file(0, fsize)

                    # Print some debugging info:
                    if miscutils.fwdebug_check(9, "HTTP_UTILS_DEBUG"):
                        miscutils.fwdebug_print("\n")
                        for lines in traceback.format_stack():
                            for L in lines.split('\n'):
                                if L.strip() != '':
                                    miscutils.fwdebug_print("call stack: %s" % L)

                    if miscutils.fwdebug_check(3, "HTTP_UTILS_DEBUG"):
                        miscutils.fwdebug_print("Copy info: %s %s %s %s %s %s" % (HttpUtils.copyfiles_called,
                                                                                  fdict['filename'],
                                                                                  fsize,
                                                                                  copy_time,
                                                                                  time.time(),
                                                                                  'toarchive' if isurl_dst else 'fromarchive'))

                    if copy_time is None:
                        copy_time = 0

                    if isurl_dst:
                        num_copies_to_archive += 1
                        total_copy_time_to_archive += copy_time
                    else:
                        num_copies_from_archive += 1
                        total_copy_time_from_archive += copy_time

                except Exception as err:
                    status = 1
                    if tstats is not None:
                        tstats.stat_end_file(1, fsize)
                    filelist[filename]['err'] = str(err)
                    miscutils.fwdebug_print(str(err))

        finally:
            print "[Copy summary] copy_batch:%d  file_copies_to_archive:%d time_to_archive:%.3f copies_from_archive:%d time_from_archive:%.3f  end_time_for_batch:%.3f" % \
            (HttpUtils.copyfiles_called, num_copies_to_archive, total_copy_time_to_archive, num_copies_from_archive, total_copy_time_from_archive, time.time())

        HttpUtils.copyfiles_called += 1
        return (status, filelist)

    def test_copyfiles(self):
        # Keeping these copies in separate lists since otherwise can't ensure the order they
        # would be copied in.
        test_filelist1 = {'testfile_dh.txt': {'compression': None,
                                              'src': './testfile_dh.txt',
                                              'filename': 'testfile_dh.txt',
                                              'filesize': 12,
                                              'path': './',
                                              'dst': 'http://desar2.cosmology.illinois.edu/DESTesting/testfile_dh.txt'}}
        test_filelist2 = {'testfile_dh2.txt': {'compression': None,
                                               'src': 'http://desar2.cosmology.illinois.edu/DESTesting/testfile_dh.txt',
                                               'filename': '20130716_bpm_10.fits',
                                               'filesize': 16781760,
                                               'path': 'refacthome/OPS/cal/bpm/',
                                               'dst': 'test_dh/testfile_dh2.txt'}}
        test_filelist3 = {'testfile_dh3.txt': {'compression': None,
                                               'src': './testfile_dh.txt',
                                               'filename': 'testfile_dh.txt',
                                               'filesize': 12,
                                               'path': './',
                                               'dst': 'http://desar2.cosmology.illinois.edu/DESTesting/bar/testfile_dh3.txt'}}
        test_filelist4 = {'testfile_dh4.txt': {'compression': None,
                                               'dst': 'test_dh/testfile_dh3.txt',
                                               'filename': 'testfile_dh.txt',
                                               'filesize': 12,
                                               'path': './',
                                               'src': 'http://desar2.cosmology.illinois.edu/DESTesting/bar/testfile_dh3.txt'}}
        test_filelist5 = {'testfile_dh5.txt': {'compression': None,
                                               'dst': 'test_dh/testfile_dh5.txt',
                                               'filename': 'testfile_dh.txt',
                                               'filesize': 12,
                                               'path': './',
                                               'src': 'http://desar2.cosmology.illinois.edu/DESTesting/bar/arggdfsbqwr.txt'}}

        shutil.rmtree('test_dh', True)
        with open("testfile_dh.txt", "w") as f: f.write("hello, world")

        self.copyfiles(test_filelist1, None)
        self.copyfiles(test_filelist2, None)
        if os.path.isfile('test_dh/testfile_dh2.txt') and os.path.getsize('test_dh/testfile_dh2.txt') == 12:
            print "Transfer of test_dh2.txt seems ok"
        else:
            miscutils.fwdie("Transfer of test_dh2.txt failed", fmdefs.FM_EXIT_FAILURE)
        self.run_curl_command("curl -K - -S -s -X DELETE http://desar2.cosmology.illinois.edu/DESTesting/testfile_dh.txt")

        self.copyfiles(test_filelist3, None)
        self.copyfiles(test_filelist4, None)
        if os.path.isfile('test_dh/testfile_dh3.txt') and os.path.getsize('test_dh/testfile_dh3.txt') == 12:
            print "Transfer of test_dh3.txt (with an intermediate dir) seems ok"
        else:
            miscutils.fwdie("Transfer of test_dh3.txt (with an intermediate dir) failed", fmdefs.FM_EXIT_FAILURE)
        self.run_curl_command("curl -K - -S -s -X DELETE http://desar2.cosmology.illinois.edu/DESTesting/bar/testfile_dh3.txt")
        self.run_curl_command("curl -K - -S -s -X DELETE http://desar2.cosmology.illinois.edu/DESTesting/bar/")

        (status, filelist5_out) = self.copyfiles(test_filelist5, None, secondsBetweenRetriesC=1, numTriesC=2)
        if filelist5_out['testfile_dh5.txt'].has_key('err') and filelist5_out['testfile_dh5.txt']['err'] == 'File operation failed with return code 22, http status 404.':
            print "Test of failed GET seems ok."
        else:
            miscutils.fwdie("Test of failed GET failed", fmdefs.FM_EXIT_FAILURE)

        os.system("rm -f ./testfile_dh.txt")
        shutil.rmtree('test_dh', True)


if __name__ == "__main__":
    # To run these unit tests first copy .desservices.ini_example to .desservices.ini
    # in test_http_utils/, and set user and passwd. Then use this command:
    #    python ./http_utils.py -v
    import doctest
    doctest.testmod()
    C = HttpUtils('test_http_utils/.desservices.ini', 'file-http')
    C.test_copyfiles()
    # miscutils.fwdie("Test miscutils.fwdie", fmdefs.FM_EXIT_FAILURE)
