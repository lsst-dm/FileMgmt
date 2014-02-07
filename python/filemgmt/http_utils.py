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

from processingfw.pfwdefs import *
import coreutils.serviceaccess as serviceaccess
import subprocess

from coreutils.miscutils import *
from filemgmt.filemgmt_defs import *

import re

class HttpUtils():
    def __init__(self):
        """Get password for curl and initialize existing_directories variable.

        >>> os.environ['DES_SERVICES'] = 'test_http_utils/.desservices.ini'
        >>> C = HttpUtils()
        >>> len(C.curl_password)
        25"""
        self.curl_password = None
        try:
            # Parse the .desservices.ini file:
            auth_params = serviceaccess.parse(False,'file-http')

            # Create the user/password switch:
            self.curl_password = "-u %s:%s\n"%(auth_params['user'],auth_params['passwd'])
        except Exception as err:
            fwdie("Unable to get curl password (%s)" % err, PF_EXIT_FAILURE)
        
        self.existing_directories = set()


    def check_url(self,P):
        """See if P is a url.

        >>> C = HttpUtils()
        >>> C.check_url("http://desar2.cosmology.illinois.edu")
        ('http://desar2.cosmology.illinois.edu', True)
        >>> C.check_url("hello")
        ('hello', False)"""
        if re.match("^https?:",P):
            return (P,True)
        return (P,False)


    def run_curl_command(self, cmd, isTest=False, useShell=False):
        """Run curl command with password given on stdin.

        >>> ignore = os.path.isfile('./hello.txt') and os.remove('./hello.txt')
        >>> C = HttpUtils()
        >>> C.run_curl_command("curl -f -S -s -K - -X PUT -w 'http_code: %{http_code}\\n' -o /dev/null -T test_http_utils/hello.txt http://desar2.cosmology.illinois.edu/DESTesting/hello.txt",isTest=True,useShell=True)
        http_code: 201
        >>> C.run_curl_command("curl -f -S -s -K - -O http://desar2.cosmology.illinois.edu/DESTesting/hello.txt",isTest=True)
        >>> os.path.isfile('./hello.txt')
        True
        >>> os.remove('./hello.txt')
        >>> C.run_curl_command("curl -f -S -s -K - -X DELETE http://desar2.cosmology.illinois.edu/DESTesting/hello.txt",isTest=True)
        """
        if not isTest: print cmd
        assert '-K - ' in cmd
        process = 0
        if not useShell:
            process = subprocess.Popen(cmd.split(), shell=False, stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        else:
            process = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        print process.communicate(self.curl_password)[0],
        if process.returncode != 0:
            fwdie("File operation failed with return code %d." % process.returncode, PF_EXIT_FAILURE)

    def create_http_intermediate_dirs(self,f):
        """Create all directories that are valid prefixes of the URL *f*.

        >>> C = HttpUtils()
        >>> C.create_http_intermediate_dirs("http://desar2.cosmology.illinois.edu/DESTesting/bar/testfile_dh.txt")
        curl -f -S -s -K - -w 'http_code: %{http_code}\\n' -o /dev/null -X MKCOL http://desar2.cosmology.illinois.edu/DESTesting
        http_code: 301
        curl -f -S -s -K - -w 'http_code: %{http_code}\\n' -o /dev/null -X MKCOL http://desar2.cosmology.illinois.edu/DESTesting/bar
        http_code: 201
        >>> C.run_curl_command("curl -K - -f -S -s -X DELETE http://desar2.cosmology.illinois.edu/DESTesting/bar/", isTest=True)
        """
        m = re.match("(http://[^/]+)(/.*)", f)
        for x in get_list_directories([ m.group(2) ]):
            if x not in self.existing_directories:
                self.run_curl_command("curl -f -S -s -K - -w 'http_code: %%{http_code}\\n' -o /dev/null -X MKCOL %s" % m.group(1)+x, useShell=True)
                self.existing_directories.add(x)

    def copyfiles(self,filelist):
        """ Copies files in given src,dst in filelist """
        for filename, fdict in filelist.items():
            try:
                (src,isurl_src) = self.check_url(fdict['src'])
                (dst,isurl_dst) = self.check_url(fdict['dst'])
                if (isurl_src and isurl_dst) or (not isurl_src and not isurl_dst):
                    fwdie("Exactly one of isurl_src and isurl_dst has to be true (values: %s %s %s %s)"
                          % (isurl_src, src, isurl_dst, dst), PF_EXIT_FAILURE)
                common_switches = '-f -S -s -K -'
                if not isurl_dst and not os.path.exists(dst):
                    path = os.path.dirname(dst)
                    if len(path) > 0 and not os.path.exists(path):
                        coremakedirs(path)
                        coremakedirs(path)
                    self.run_curl_command("curl %s %s -o %s" % (common_switches,src,dst))
                elif isurl_dst:
                    self.create_http_intermediate_dirs(dst)
                    self.run_curl_command("curl %s -T %s -X PUT %s" % (common_switches,src,dst))
            except Exception as err:
                filelist[filename]['err'] = str(err)
        return filelist
    
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

        shutil.rmtree('test_dh', True)
        with open("testfile_dh.txt","w") as f: f.write("hello, world")
    
        self.copyfiles(test_filelist1)
        self.copyfiles(test_filelist2)
        self.copyfiles(test_filelist3)
        self.copyfiles(test_filelist4)
        if os.path.isfile('test_dh/testfile_dh2.txt') and os.path.getsize('test_dh/testfile_dh2.txt') == 12:
            print "Transfer of test_dh2.txt seems ok"
        else:
            fwdie("Transfer of test_dh2.txt failed", PF_EXIT_FAILURE)
        if os.path.isfile('test_dh/testfile_dh3.txt') and os.path.getsize('test_dh/testfile_dh3.txt') == 12:
            print "Transfer of test_dh3.txt (with an intermediate dir) seems ok"
        else:
            fwdie("Transfer of test_dh3.txt (with an intermediate dir) failed", PF_EXIT_FAILURE)
#        self.run_curl_command("curl -K - -S -s -X DELETE http://desar2.cosmology.illinois.edu/DESTesting/testfile_dh.txt")
#        self.run_curl_command("curl -K - -S -s -X DELETE http://desar2.cosmology.illinois.edu/DESTesting/bar/testfile_dh3.txt")
#        self.run_curl_command("curl -K - -S -s -X DELETE http://desar2.cosmology.illinois.edu/DESTesting/bar/")

        shutil.rmtree('test_dh', True)
        os.system("rm -f ./testfile_dh.txt")
        os.system("rm -f ./testfile_dh3.txt")

if __name__ == "__main__":
    import doctest
    doctest.testmod()
    C = HttpUtils()
    C.test_copyfiles()
