/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.messaging.kafka;

import java.util.List;
import java.util.logging.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.state.ConsistentRegionContext;

@InputPorts(@InputPortSet(cardinality=1, optional=false, 
	description="The tuples arriving on this port are expected to contain three attributes \\\"key\\\", \\\"topic\\\" and \\\"message\\\". " +
			"Out of these \\\"message\\\", is a required attribute."))
@PrimitiveOperator(name=KafkaSink.OPER_NAME, description=KafkaSink.DESC)
@Icons(location16="icons/KafkaProducer_16.gif", location32="icons/KafkaProducer_32.gif")
public class KafkaSink extends KafkaBaseOper {
	
	
	static final String OPER_NAME =  "KafkaProducer";
	
	private static final Logger trace = Logger.getLogger(KafkaSink.class.getName());

	
	@Parameter(name="topic", cardinality=-1, optional=true, 
			description="Topic to be published to. A topic can also be specified as an input stream attribute.")
	public void setTopic(List<String> values) {
		if(values!=null)
			topics.addAll(values);
	}	
	@Parameter(optional=true, description="Name of the attribute for the topic. Default is \\\"topic\\\"")
	public void setTopicAttribute(String value) {
		topicAH.setName(value);
	}
	
	@ContextCheck(compile=true)
	public static boolean topicChecker(OperatorContextChecker checker) {
		return checker.checkExcludedParameters("topic", "topicAttribute") &&
			   checker.checkExcludedParameters("topicAttribute", "topic");
	}

	//consistent region checks
	@ContextCheck(compile = true)
	public static void checkInConsistentRegion(OperatorContextChecker checker) {
		ConsistentRegionContext consistentRegionContext = 
				checker.getOperatorContext().getOptionalContext(ConsistentRegionContext.class);
		
		if(consistentRegionContext != null && consistentRegionContext.isStartOfRegion()) {
			checker.setInvalidContext( OPER_NAME + " operator cannot be placed at the start of consistent region.", 
					new String[] {});
		}
	}

	@Override
	public void initialize(OperatorContext context)
			throws Exception {
		super.initialize(context);
		super.initSchema(getInput(0).getStreamSchema());
		
		if(topics.size() == 0 && !topicAH.isAvailable())
			throw new Exception("Topic has not been specified. Specify either the \"topicAttribute\" or \"topic\" parameters.");
		
		if(keyAH.isAvailable() && ( keyAH.isString() != messageAH.isString())) {
			throw new Exception("Key and Message attributes must have compatible types.");
		}
		
		if(!topics.isEmpty())
			trace.log(TraceLevel.INFO, "Topics: " + topics.toString());
		//TODO: check for minimum properties
		trace.log(TraceLevel.INFO, "Initializing producer");
		client.initProducer();
	}
	
	@Override
	public void process(StreamingInput<Tuple> stream, Tuple tuple)
			throws Exception {
		try {
		
			if(trace.isLoggable(TraceLevel.DEBUG))
				trace.log(TraceLevel.DEBUG, "Sending message: " + tuple);
			
			if(!topics.isEmpty()) 
				client.send(tuple, topics);
			else 
				client.send(tuple);
		}catch(Exception e) {
			//ideally we should not get here since the kafka client doesnt seem to be throwing any exceptions
			trace.log(TraceLevel.ERROR, "Could not send message: " + tuple, e);
		}
	}	
	
	public static final String DESC = 
			"This operator acts as a Kafka producer sending tuples as mesages to a Kafka broker. " + 
			"The broker is assumed to be already configured and running. " +
			"The incoming stream can have three attributes: topic, key and message. " +
			"The message is a required attribute. If the key attribute is not specified, the message is used as the key. " +
			"A topic can be specified as either an input stream attribute or as a parameter." +
			"\\n\\n**Behavior in a Consistent Region**" + 
			"\\nThis operator can participate in a consistent region.  This operator cannot be placed at the start of a consistent region."
			;
}
