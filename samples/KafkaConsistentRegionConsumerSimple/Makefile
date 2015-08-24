# Copyright (C) 2015, International Business Machines Corporation  
# All Rights Reserved      

.PHONY: all clean 

TOOLKIT_NAME=com.ibm.streamsx.messaging
STREAMS_MESSAGING_TOOLKIT ?= $(shell  ([ -e ../../$(TOOLKIT_NAME)/toolkit.xml ] && echo ../../$(TOOLKIT_NAME)) ||\
						          echo $(STREAMS_STUDIO_SPL_PATH) ||\
						          ([ -e "../$(TOOLKIT_NAME)" ] && echo ../$(TOOLKIT_NAME)) ||\
									echo $(STREAMS_INSTALL)/toolkits/$(TOOLKIT_NAME))


SPLC_FLAGS ?= -a --data-directory data 
SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?= -t $(STREAMS_MESSAGING_TOOLKIT)
SPL_MAIN_COMPOSITE = application::ConsistentRegionSimple

all: distributed
 
data:
	mkdir data

standalone: data
	$(SPLC) $(SPLC_FLAGS) -T -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS)

distributed: data
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS)

clean: 
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)