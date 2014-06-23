# Copyright (C) 2011,2013,2014, International Business Machines Corporation. 
# All Rights Reserved.

.PHONY: all standalone distributed clean 

STREAMS_MSG_TOOLKIT ?=../../com.ibm.streamsx.messaging
SPLC_FLAGS ?= -a -t $(STREAMS_MSG_TOOLKIT)
SPLC = $(STREAMS_INSTALL)/bin/sc
SPL_CMD_ARGS ?= 
SPL_MAIN_COMPOSITE ?= com.ibm.streamsx.messaging.sample.kafka::KafkaConsumerSample

all: standalone


standalone:
	$(SPLC) $(SPLC_FLAGS) -T -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS)

distributed:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS)

clean: 
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)
	rm -rf output