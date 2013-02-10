# $Id: Makefile 10292 2013-01-07 17:54:32Z mgower $
# $Rev:: 10292                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2013-01-07 11:54:32 #$:  # Date of last commit.

# Override any environment variable called "prefix".  This would normally be
# set to /usr/local, but that's not reasonable for this package.
prefix=

upsdir=$(prefix)/ups

# INSTALL_FILES is used to remove installed files from a directory that could
# have files installed from other projects, so this wildcard needs to be
# resolved here against the local directory.
INSTALL_FILES:=$(wildcard *.table)

SHELL=/bin/sh

.PHONY: all clean install uninstall

all:
	@true

install:
	@if [ "$(upsdir)" = "/ups" ]; then \
	    echo "FileMgmt:  usage: make prefix=<install_dir> install"; \
	    false; \
	fi #$(MAKE) -- Do this test even with make -n
	@echo "FileMgmt: installing to $(upsdir)";
	mkdir -p $(upsdir)
	cp $(INSTALL_FILES) $(upsdir)

uninstall:
	@if [ "$(upsdir)" = "/ups" ]; then \
	    echo "FileMgmt:  usage: make prefix=<install_dir> install"; \
	    false; \
	fi #$(MAKE) -- Do this test even with make -n
	@echo "FileMgmt: uninstalling from $(upsdir)";
	-cd $(upsdir) && rm $(INSTALL_FILES)
	-rmdir $(upsdir)

clean:
	@true
