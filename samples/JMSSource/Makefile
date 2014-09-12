# Copyright (C)2014, International Business Machines Corporation and  
# others. All Rights Reserved.                        
.PHONY: all clean

SPLC_FLAGS = -a -z -t ../../com.ibm.streamsx.messaging

SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?=
SPL_MAIN_COMPOSITE = com.ibm.streamsx.messaging.sample.jms::JMSSourceMain
OUTPUT_DIR = output

all: distributed

standalone:
	$(SPLC) $(SPLC_FLAGS) -T -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS)

distributed:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) --output-dir ./$(OUTPUT_DIR) $(SPL_CMD_ARGS)

clean:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)


