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

tests=BasicFunctionTestMain

rntest=./scripts/testRunner.sh
ftest=./scripts/expectFail.sh

all: ${tests}
	@echo "Tests Passed"

compile: ${tests}

%TestMain:
	${sc} -T ${args} --output-directory ${outputdir}/$@ -M ${ns}::$@
	${rntest} ${outputdir}/$@/bin/standalone ${ns}::$@.producerProperties=producer.properties ${ns}::$@.consumerProperties=consumer.properties


%Failtest:
	${ftest} ${sc} -T ${args} --output-directory ${outputdir}/$@ -M ${ns}::$@ 

clean:
	rm -rf ${outputdir}
