//
// *******************************************************************************
// * Copyright (C)2014, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
//
package com.ibm.streamsx.messaging.kafka;


import java.util.List;
import java.util.logging.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;

@OutputPorts(@OutputPortSet(cardinality=1, optional=false, 
	description="Messages received from Kafka are sent on this output port."))
@PrimitiveOperator(name="KafkaReceiver", description=KafkaSource.DESC)
public class KafkaSource extends KafkaBaseOper {

	private int threadsPerTopic = 1;

	private static Logger trace = Logger.getLogger(KafkaSource.class.getName());

	@Override
	public void initialize(OperatorContext context)
			throws Exception {
		super.initialize(context);
		super.initSchema(getOutput(0).getStreamSchema());

		if(threadsPerTopic < 1) 
			throw new IllegalArgumentException("Number of threads per topic cannot be less than one: " + threadsPerTopic);
	}

	@Override
	public void allPortsReady() throws Exception {
		//initialize the client
		trace.log(TraceLevel.INFO, "Initializing client");
		client.initConsumer(getOutput(0), getOperatorContext().getThreadFactory(), topics, threadsPerTopic);
	}

	@Parameter(name="threadsPerTopic", optional=true, 
			description="Number of threads per topic. Default is 1.")
	public void setThreadsPerTopic(int value) {
		this.threadsPerTopic = value;
	}	
	@Parameter(name="topic", cardinality=-1, optional=false, 
			description="Topic to be subscribed to.")
	public void setTopic(List<String> values) {
		if(values!=null)
			topics.addAll(values);
	}	

	public static final String DESC = 
			"This operator acts as a Kafka consumer recieving messages for one or more topics. " +
			"Note that there may be multiple threads receiving messages depending on the configuration specified. " +
			"Ordering of messages is not guaranteed."
					;

}

