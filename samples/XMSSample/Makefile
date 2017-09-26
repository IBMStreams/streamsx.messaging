# Copyright (C)2016, International Business Machines Corporation and
# others. All Rights Reserved.       
                
.PHONY: all clean

STREAMS_MESSAGING_TOOLKIT ?= ../../com.ibm.streamsx.messaging:${STREAMS_INSTALL}/toolkits/com.ibm.streamsx.messaging

SPLC_FLAGS ?= -a -t ${STREAMS_MESSAGING_TOOLKIT} --data-directory=data

SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?=
SPL_MAIN_COMPOSITE = com.ibm.streamsx.messaging.sample.xms::XMSSample
OUTPUT_DIR = output

all: 
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) --output-dir ./$(OUTPUT_DIR) $(SPL_CMD_ARGS)

clean:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)


