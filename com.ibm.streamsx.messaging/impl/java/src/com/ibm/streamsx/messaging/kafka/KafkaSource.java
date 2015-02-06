/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.messaging.kafka;


import java.util.List;
import java.util.logging.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.state.ConsistentRegionContext;

@OutputPorts(@OutputPortSet(cardinality=1, optional=false, 
	description="Messages received from Kafka are sent on this output port."))
@PrimitiveOperator(name=KafkaSource.OPER_NAME, description=KafkaSource.DESC)
@Icons(location16="icons/KafkaConsumer_16.gif", location32="icons/KafkaConsumer_32.gif")
public class KafkaSource extends KafkaBaseOper {

	static final String OPER_NAME = "KafkaConsumer";
	private int threadsPerTopic = 1;

	private static Logger trace = Logger.getLogger(KafkaSource.class.getName());

	//consistent region checks
	@ContextCheck(compile = true)
	public static void checkInConsistentRegion(OperatorContextChecker checker) {
		ConsistentRegionContext consistentRegionContext = 
				checker.getOperatorContext().getOptionalContext(ConsistentRegionContext.class);
		
		if(consistentRegionContext != null ) {
			checker.setInvalidContext( OPER_NAME + " operator cannot be used inside a consistent region.", 
					new String[] {});
		}
	}

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
			"This operator acts as a Kafka consumer receiving messages for one or more topics. " +
			"Note that there may be multiple threads receiving messages depending on the configuration specified. " +
			"Ordering of messages is not guaranteed." + 
			"\\n\\n**Behavior in a Consistent Region**" + 
			"\\nThis operator cannot be used inside a consistent region."
			;

}

