# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit. 
# $LastChangedDate::                      $:  # Date of last commit.

SHELL=/bin/sh

build:
	@echo "FileMgmt: Ready to install"

install:
ifndef INSTALL_ROOT
	@echo "FileMgmt: Must define INSTALL_ROOT"
	false
endif
	@echo "FileMgmt: Installing to ${INSTALL_ROOT}"
	-mkdir -p ${INSTALL_ROOT}
	-mkdir -p ${INSTALL_ROOT}/python
	-rsync -Caq python/filemgmt ${INSTALL_ROOT}/python
	@echo "Make sure ${INSTALL_ROOT}/python is in PYTHONPATH"

test:
	@echo "FileMgmt: tests are currently not available"

clean:
	@echo "FileMgmt: no cleanup defined"
