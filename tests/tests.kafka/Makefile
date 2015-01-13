#
# *******************************************************************************
# * Copyright (C)2014, International Business Machines Corporation and *
# * others. All Rights Reserved. *
# *******************************************************************************
#

sc=${STREAMS_INSTALL}/bin/sc
args=-t ../../com.ibm.streamsx.messaging
ns=com.ibm.streamsx.messaging.kafka.tests
outputdir=./output

tests=BasicFunctionTestMain BasicFunctionBlobTestMain ConsumerGroupTestMain ConsumerMultiGroupTestMain ProducerTopicTestFailMain ConsistentRegionProducerFailMain ConsistentRegionConsumerFailMain

rntest=./scripts/testRunner.sh
ftest=./scripts/expectFail.sh
pwd=${PWD}

all: ${tests}
	@echo "Tests Passed"

compile: ${tests}

%TestMain:
	${sc} -T ${args} --output-directory ${outputdir}/$@ -M ${ns}::$@
	${rntest} ${outputdir}/$@/bin/standalone ${ns}::$@.producerProperties=${pwd}/data/producer.properties ${ns}::$@.consumerProperties=${pwd}/data/consumer.properties


%FailMain:
	${ftest} ${sc} ${args} --output-directory ${outputdir}/$@ -M ${ns}::$@ 

clean:
	rm -rf ${outputdir}
