# Copyright (C) 2015, International Business Machines Corporation  
# All Rights Reserved      

.PHONY: all clean 

STREAMS_MESSAGING_TOOLKIT ?= ../../com.ibm.streamsx.messaging:${STREAMS_INSTALL}/toolkits/com.ibm.streamsx.messaging


SPLC_FLAGS ?= -a --data-directory data 
SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?= -t $(STREAMS_MESSAGING_TOOLKIT)
SPL_MAIN_COMPOSITE = application::ConsistentRegionSimple

all: data
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS)
 
data:
	mkdir data

clean: 
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)