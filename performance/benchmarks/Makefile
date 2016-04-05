# Copyright (C)2014, International Business Machines Corporation and  
# others. All Rights Reserved.                        
.PHONY: all ConsumerBlobBenchmark ConsumerRStringBenchmark ParallelConsumerBenchmark ProducerBlobBenchmark ProducerRStringBenchmark clean-ConsumerBlobBenchmark clean-ConsumerRStringBenchmark clean-ParallelConsumerBenchmark clean-ProducerBlobBenchmark clean-ProducerRStringBenchmark clean-appset check-tk

CPAPPWD=$(shell pwd)

SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?=

STREAMS_MESSAGING_TOOLKIT ?= ../../com.ibm.streamsx.messaging:${STREAMS_INSTALL}/toolkits/com.ibm.streamsx.messaging:$(STREAMS_STUDIO_SPL_PATH)

SPLC_FLAGS ?= -a -t ${STREAMS_MESSAGING_TOOLKIT}
SPLC_FLAGS_ConsumerBlobBenchmark     ?= $(SPLC_FLAGS) 
SPLC_FLAGS_ConsumerRStringBenchmark  ?= $(SPLC_FLAGS)
SPLC_FLAGS_ParallelConsumerBenchmark        ?= $(SPLC_FLAGS)
SPLC_FLAGS_ProducerBlobBenchmark ?= $(SPLC_FLAGS)
SPLC_FLAGS_ProducerRStringBenchmark  ?= $(SPLC_FLAGS)

SPL_MAIN_COMPOSITE_ConsumerBlobBenchmark     = com.ibm.streamsx.messaging.perf.kafka::ConsumerBlobBenchmark
SPL_MAIN_COMPOSITE_ConsumerRStringBenchmark  = com.ibm.streamsx.messaging.perf.kafka::ConsumerRStringBenchmark
SPL_MAIN_COMPOSITE_ParallelConsumerBenchmark        = com.ibm.streamsx.messaging.perf.kafka::ParallelConsumerBenchmark
SPL_MAIN_COMPOSITE_ProducerBlobBenchmark = com.ibm.streamsx.messaging.perf.kafka::ProducerBlobBenchmark 
SPL_MAIN_COMPOSITE_ProducerRStringBenchmark  = com.ibm.streamsx.messaging.perf.kafka::ProducerRStringBenchmark

SPL_OUTPUT_DIR_ConsumerBlobBenchmark     = output/ConsumerBlobBenchmark
SPL_OUTPUT_DIR_ConsumerRStringBenchmark  = output/ConsumerRStringBenchmark
SPL_OUTPUT_DIR_ParallelConsumerBenchmark        = output/ParallelConsumerBenchmark
SPL_OUTPUT_DIR_ProducerBlobBenchmark = output/ProducerBlobBenchmark
SPL_OUTPUT_DIR_ProducerRStringBenchmark  = output/ProducerRStringBenchmark

SPL_DATA_DIR_ConsumerBlobBenchmark     = $(CPAPPWD)/data/ConsumerBlobBenchmark
SPL_DATA_DIR_ConsumerRStringBenchmark  = $(CPAPPWD)/data/ConsumerRStringBenchmark
SPL_DATA_DIR_ParallelConsumerBenchmark        = $(CPAPPWD)/data/ParallelConsumerBenchmark
SPL_DATA_DIR_ProducerBlobBenchmark = $(CPAPPWD)/data/ProducerBlobBenchmark
SPL_DATA_DIR_ProducerRStringBenchmark  = $(CPAPPWD)/data/ProducerRStringBenchmark

all: ConsumerBlobBenchmark ConsumerRStringBenchmark ParallelConsumerBenchmark ProducerBlobBenchmark ProducerRStringBenchmark $(SPL_SUBMIT_APPSET)

ConsumerBlobBenchmark: check-tk
	mkdir -p $(SPL_DATA_DIR_ConsumerBlobBenchmark)
	$(SPLC) $(SPLC_FLAGS_ConsumerBlobBenchmark) -M $(SPL_MAIN_COMPOSITE_ConsumerBlobBenchmark) \
          --output-dir=$(SPL_OUTPUT_DIR_ConsumerBlobBenchmark) --data-dir=$(SPL_DATA_DIR_ConsumerBlobBenchmark) $(SPL_CMD_ARGS_ConsumerBlobBenchmark)

ConsumerRStringBenchmark: check-tk
	mkdir -p $(SPL_DATA_DIR_ConsumerRStringBenchmark)
	$(SPLC) $(SPLC_FLAGS_ConsumerRStringBenchmark) -M $(SPL_MAIN_COMPOSITE_ConsumerRStringBenchmark) \
          --output-dir=$(SPL_OUTPUT_DIR_ConsumerRStringBenchmark) --data-dir=$(SPL_DATA_DIR_ConsumerRStringBenchmark) $(SPL_CMD_ARGS_ConsumerRStringBenchmark)

ParallelConsumerBenchmark: check-tk
	mkdir -p $(SPL_DATA_DIR_ParallelConsumerBenchmark)
	$(SPLC) $(SPLC_FLAGS_ParallelConsumerBenchmark) -M $(SPL_MAIN_COMPOSITE_ParallelConsumerBenchmark) \
          --output-dir=$(SPL_OUTPUT_DIR_ParallelConsumerBenchmark) --data-dir=$(SPL_DATA_DIR_ParallelConsumerBenchmark) $(SPL_CMD_ARGS_ParallelConsumerBenchmark)

ProducerBlobBenchmark: check-tk
	mkdir -p $(SPL_DATA_DIR_ProducerBlobBenchmark)
	$(SPLC) $(SPLC_FLAGS_ProducerBlobBenchmark) -M $(SPL_MAIN_COMPOSITE_ProducerBlobBenchmark) \
          --output-dir=$(SPL_OUTPUT_DIR_ProducerBlobBenchmark) --data-dir=$(SPL_DATA_DIR_ProducerBlobBenchmark) $(SPL_CMD_ARGS_ProducerBlobBenchmark)

ProducerRStringBenchmark: check-tk
	mkdir -p $(SPL_DATA_DIR_ProducerRStringBenchmark)
	$(SPLC) $(SPLC_FLAGS_ProducerRStringBenchmark) -M $(SPL_MAIN_COMPOSITE_ProducerRStringBenchmark) \
          --output-dir=$(SPL_OUTPUT_DIR_ProducerRStringBenchmark) --data-dir=$(SPL_DATA_DIR_ProducerRStringBenchmark) $(SPL_CMD_ARGS_ProducerRStringBenchmark)

$(SPL_SUBMIT_APPSET): 
	echo $(SPL_OUTPUT_DIR_ConsumerBlobBenchmark)/$(SPL_MAIN_COMPOSITE_ConsumerBlobBenchmark).adl > $(SPL_SUBMIT_APPSET)
	echo $(SPL_OUTPUT_DIR_ConsumerRStringBenchmark)/$(SPL_MAIN_COMPOSITE_ConsumerRStringBenchmark).adl >> $(SPL_SUBMIT_APPSET)
	echo $(SPL_OUTPUT_DIR_ParallelConsumerBenchmark)/$(SPL_MAIN_COMPOSITE_ParallelConsumerBenchmark).adl >> $(SPL_SUBMIT_APPSET)
	echo $(SPL_OUTPUT_DIR_ProducerBlobBenchmark)/$(SPL_MAIN_COMPOSITE_ProducerBlobBenchmark).adl >> $(SPL_SUBMIT_APPSET)
	echo $(SPL_OUTPUT_DIR_ProducerRStringBenchmark)/$(SPL_MAIN_COMPOSITE_ProducerRStringBenchmark).adl >> $(SPL_SUBMIT_APPSET)
	sed -i 's/::/./g' $(SPL_SUBMIT_APPSET)

clean: clean-ConsumerBlobBenchmark clean-ConsumerRStringBenchmark clean-ParallelConsumerBenchmark clean-ProducerBlobBenchmark clean-ProducerRStringBenchmark clean-appset

clean-ConsumerBlobBenchmark:
	rm -rf $(SPL_DATA_DIR_ConsumerBlobBenchmark)
	$(SPLC) $(SPLC_FLAGS_ConsumerBlobBenchmark) -C -M $(SPL_MAIN_COMPOSITE_ConsumerBlobBenchmark) \
	  --output-dir=$(SPL_OUTPUT_DIR_ConsumerBlobBenchmark) --data-dir=$(SPL_DATA_DIR_ConsumerBlobBenchmark) 

clean-ConsumerRStringBenchmark:
	rm -rf $(SPL_DATA_DIR_ConsumerRStringBenchmark)
	$(SPLC) $(SPLC_FLAGS_ConsumerRStringBenchmark) -C -M $(SPL_MAIN_COMPOSITE_ConsumerRStringBenchmark) \
	  --output-dir=$(SPL_OUTPUT_DIR_ConsumerRStringBenchmark) --data-dir=$(SPL_DATA_DIR_ConsumerRStringBenchmark) 

clean-ParallelConsumerBenchmark: 
	rm -rf $(SPL_DATA_DIR_ParallelConsumerBenchmark)
	$(SPLC) $(SPLC_FLAGS_ParallelConsumerBenchmark) -C -M $(SPL_MAIN_COMPOSITE_ParallelConsumerBenchmark) \
	  --output-dir=$(SPL_OUTPUT_DIR_ParallelConsumerBenchmark) --data-dir=$(SPL_DATA_DIR_ParallelConsumerBenchmark) 

clean-ProducerBlobBenchmark: 
	rm -rf $(SPL_DATA_DIR_ProducerBlobBenchmark)
	$(SPLC) $(SPLC_FLAGS_ProducerBlobBenchmark) -C -M $(SPL_MAIN_COMPOSITE_ProducerBlobBenchmark) \
	  --output-dir=$(SPL_OUTPUT_DIR_ProducerBlobBenchmark) --data-dir=$(SPL_DATA_DIR_ProducerBlobBenchmark) 

clean-ProducerRStringBenchmark: 
	rm -rf $(SPL_DATA_DIR_ProducerRStringBenchmark)
	$(SPLC) $(SPLC_FLAGS_ProducerRStringBenchmark) -C -M $(SPL_MAIN_COMPOSITE_ProducerRStringBenchmark) \
	  --output-dir=$(SPL_OUTPUT_DIR_ProducerRStringBenchmark) --data-dir=$(SPL_DATA_DIR_ProducerRStringBenchmark) 

clean-appset:
	rm -f $(SPL_SUBMIT_APPSET)