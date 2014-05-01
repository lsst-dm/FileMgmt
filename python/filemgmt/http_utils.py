# $Id: disk_utils_local.py 18486 2014-01-29 15:58:12Z mgower $
# $Rev:: 18486                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2014-01-29 09:58:12 #$:  # Date of last commit.

"""
Routines for performing tasks on files available through http.
"""

__version__ = "$Rev: 18486 $"

import os
import shutil
import coreutils.serviceaccess as serviceaccess
import subprocess
import coreutils.miscutils as coremisc
import filemgmt.filemgmt_defs as fmdefs
import re

import traceback
import time


class HttpUtils():
    copyfiles_called = 0

    def __init__(self, des_services, des_http_section):
        """Get password for curl and initialize existing_directories variable.

        >>> C = HttpUtils('test_http_utils/.desservices.ini','file-http')
        >>> len(C.curl_password)
        25"""
        try:
            # Parse the .desservices.ini file:
            auth_params = serviceaccess.parse(des_services, des_http_section)

            # Create the user/password switch:
            self.curl_password = "-u %s:%s\n"%(auth_params['user'],auth_params['passwd'])
        except Exception as err:
            coremisc.fwdie("Unable to get curl password (%s)" % err, fmdefs.FM_EXIT_FAILURE)
        
        self.existing_directories = set()


    def check_url(self,P):
        """See if P is a url.

        >>> C = HttpUtils('test_http_utils/.desservices.ini','file-http')
        >>> C.check_url("http://desar2.cosmology.illinois.edu")
        ('http://desar2.cosmology.illinois.edu', True)
        >>> C.check_url("hello")
        ('hello', False)"""
        if re.match("^https?:",P):
            return (P,True)
        return (P,False)


    def run_curl_command(self, cmd, isTest=False, useShell=False, curlConsoleOutputFile='curl_stdout.txt', secondsBetweenRetries=15, numTries=3):
        """Run curl command with password given on stdin.

        >>> ignore = os.path.isfile('./hello.txt') and os.remove('./hello.txt')
        >>> C = HttpUtils('test_http_utils/.desservices.ini','file-http')
        >>> C.run_curl_command("curl -f -S -s -K - -X PUT -w 'http_code: %{http_code}\\n' -T test_http_utils/hello.txt http://desar2.cosmology.illinois.edu/DESTesting/hello.txt",isTest=True,useShell=True)
        >>> C.run_curl_command("curl -f -S -s -K - http://desar2.cosmology.illinois.edu/DESTesting/hello.txt",isTest=True,curlConsoleOutputFile='hello.txt')
        >>> os.path.isfile('./hello.txt')
        True
        >>> os.remove('./hello.txt')
        >>> C.run_curl_command("curl -f -S -s -K - -X DELETE http://desar2.cosmology.illinois.edu/DESTesting/hello.txt",isTest=True)
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
        assert '-K - ' in cmd
        assert '-o ' not in cmd
        cmd = re.sub("^curl ","curl -o %s " % curlConsoleOutputFile, cmd)
        process = 0
        starttime = time.time()
        for x in range(0,numTries):
          if not isTest and x == 0: coremisc.fwdebug(3, "HTTP_UTILS_DEBUG", "curl command: %s" % cmd)
          if not isTest and x > 0:
              print "repeating curl command after failure (%d): %s" % (x,cmd)
          if not useShell:
              process = subprocess.Popen(cmd.split(), shell=False, stdin=subprocess.PIPE,
                                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
          else:
              process = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE,
                                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
          curl_output = process.communicate(self.curl_password)[0] # Don't know why the -o switch doesn't cause this to go to stdout.
          if not isTest and x > 0:
              print curl_output
          if process.returncode == 0:
              break
          if x < numTries-1:    # not the last time in the loop
              time.sleep(secondsBetweenRetries)
        if process.returncode != 0:
            if os.path.isfile(curlConsoleOutputFile) and os.stat(curlConsoleOutputFile).st_size < 2000:
                with open(curlConsoleOutputFile,'r') as f:
                    for line in f:
                        print "%s: %s" % (curlConsoleOutputFile,line),
            http_match = re.search('http_code: ?(\d+)', curl_output)
            assert http_match
            raise Exception("File operation failed with return code %d, http status %s."
                            % (process.returncode,http_match.group(1)))
        if os.path.isfile('curl_stdout.txt'):
            os.remove('curl_stdout.txt')
        if not isTest:
            return time.time()-starttime

    def create_http_intermediate_dirs(self,f):
        """Create all directories that are valid prefixes of the URL *f*.

        >>> C = HttpUtils('test_http_utils/.desservices.ini','file-http')
        >>> C.create_http_intermediate_dirs("http://desar2.cosmology.illinois.edu/DESTesting/bar/testfile_dh.txt")
        >>> C.run_curl_command("curl -K - -f -S -s -X DELETE http://desar2.cosmology.illinois.edu/DESTesting/bar/", isTest=True)
        """
        # Making bar/ sometimes returns a 301 status even if there doesn't seem to be a bar/ in the directory.
        m = re.match("(http://[^/]+)(/.*)", f)
        for x in coremisc.get_list_directories([ m.group(2) ]):
            if x not in self.existing_directories:
                self.run_curl_command("curl -f -S -s -K - -w 'http_code: %%{http_code}\\n' -X MKCOL %s" % m.group(1)+x, useShell=True)
                self.existing_directories.add(x)

    def copyfiles(self,filelist, tstats, secondsBetweenRetriesC=15, numTriesC=3):
        """ Copies files in given src,dst in filelist """
        num_copies_from_archive = 0
        num_copies_to_archive = 0
        total_copy_time_from_archive = 0.0
        total_copy_time_to_archive = 0.0
        status = 0

        for filename, fdict in filelist.items():
            fsize = 0
            if 'filesize' in fdict:
                fsize = fdict['filesize']
            try:
                (src,isurl_src) = self.check_url(fdict['src'])
                (dst,isurl_dst) = self.check_url(fdict['dst'])
                if (isurl_src and isurl_dst) or (not isurl_src and not isurl_dst):
                    coremisc.fwdie("Exactly one of isurl_src and isurl_dst has to be true (values: %s %s %s %s)"
                          % (isurl_src, src, isurl_dst, dst), fmdefs.FM_EXIT_FAILURE)
                common_switches = "-f -S -s -K - -w \'http_code:%{http_code}\\n\'"
                copy_time = None
                if not isurl_dst and not os.path.exists(dst):
                    if tstats is not None:
                        tstats.stat_beg_file(filename)
                    path = os.path.dirname(dst)
                    if len(path) > 0 and not os.path.exists(path):
                        coremisc.coremakedirs(path)
                    copy_time = self.run_curl_command("curl %s %s" % (common_switches,src), curlConsoleOutputFile=dst,
                                                      useShell=True, secondsBetweenRetries=secondsBetweenRetriesC, numTries=numTriesC)
                    if tstats is not None:
                        tstats.stat_end_file(0, fsize)
                elif isurl_dst:
                    if tstats is not None:
                        tstats.stat_beg_file(filename)
                    self.create_http_intermediate_dirs(dst)
                    copy_time = self.run_curl_command("curl %s -T %s -X PUT %s" % (common_switches,src,dst), useShell=True,
                                                      secondsBetweenRetries=secondsBetweenRetriesC, numTries=numTriesC)
                    if tstats is not None:
                        tstats.stat_end_file(0, fsize)

                # Print some debugging info:
                coremisc.fwdebug(3, "HTTP_UTILS_DEBUG", "\n")
                for lines in traceback.format_stack():
                    for L in lines.split('\n'):
                        if L.strip() != '':
                            coremisc.fwdebug(3, "HTTP_UTILS_DEBUG", "call stack: %s" % L)
                coremisc.fwdebug(3, "HTTP_UTILS_DEBUG", "Copy info: %d %s %d %s %s %s" % (HttpUtils.copyfiles_called,
                                                                                 fdict['filename'],
                                                                                 fdict['filesize'],
                                                                                 copy_time,
                                                                                 time.time(),
                                                                                 'toarchive' if isurl_dst else 'fromarchive'))
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
                print str(err)
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
        with open("testfile_dh.txt","w") as f: f.write("hello, world")

        self.copyfiles(test_filelist1)
        self.copyfiles(test_filelist2)
        if os.path.isfile('test_dh/testfile_dh2.txt') and os.path.getsize('test_dh/testfile_dh2.txt') == 12:
            print "Transfer of test_dh2.txt seems ok"
        else:
            coremisc.fwdie("Transfer of test_dh2.txt failed", fmdefs.FM_EXIT_FAILURE)
        self.run_curl_command("curl -K - -S -s -X DELETE http://desar2.cosmology.illinois.edu/DESTesting/testfile_dh.txt")

        self.copyfiles(test_filelist3)
        self.copyfiles(test_filelist4)
        if os.path.isfile('test_dh/testfile_dh3.txt') and os.path.getsize('test_dh/testfile_dh3.txt') == 12:
            print "Transfer of test_dh3.txt (with an intermediate dir) seems ok"
        else:
            coremisc.fwdie("Transfer of test_dh3.txt (with an intermediate dir) failed", fmdefs.FM_EXIT_FAILURE)
        self.run_curl_command("curl -K - -S -s -X DELETE http://desar2.cosmology.illinois.edu/DESTesting/bar/testfile_dh3.txt")
        self.run_curl_command("curl -K - -S -s -X DELETE http://desar2.cosmology.illinois.edu/DESTesting/bar/")

        (status, filelist5_out) = self.copyfiles(test_filelist5,secondsBetweenRetriesC=1,numTriesC=2)
        if filelist5_out['testfile_dh5.txt'].has_key('err') and filelist5_out['testfile_dh5.txt']['err'] == 'File operation failed with return code 22, http status 404.':
            print "Test of failed GET seems ok."
        else:
            coremisc.fwdie("Test of failed GET failed", fmdefs.FM_EXIT_FAILURE)

        os.system("rm -f ./testfile_dh.txt")
        shutil.rmtree('test_dh', True)


if __name__ == "__main__":
    # To run these unit tests first copy .desservices.ini_example to .desservices.ini
    # in test_http_utils/, and set user and passwd. Then use this command:
    #    python ./http_utils.py -v
    import doctest
    doctest.testmod()
    C = HttpUtils('test_http_utils/.desservices.ini','file-http')
    C.test_copyfiles()
    # coremisc.fwdie("Test coremisc.fwdie", fmdefs.FM_EXIT_FAILURE)
