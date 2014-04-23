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
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;

@InputPorts(@InputPortSet(cardinality=1, optional=false, 
	description="The tuples arriving on this port are expected to contain three attributes \\\"key\\\", \\\"topic\\\" and \\\"message\\\". " +
			"Out of these \\\"message\\\", is a required attribute."))
@PrimitiveOperator(description=KafkaSink.DESC)
public class KafkaSink extends KafkaBaseOper {
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
	
	@Override
	public void initialize(OperatorContext context)
			throws Exception {
		super.initialize(context);
		super.initSchema(getInput(0).getStreamSchema());
		
		if(topics.size() > 0 && topicAH.isAvailable())
			throw new Exception("Topic attribute and topic parameter values have both been set. Please use only one.");
		if(topics.size() == 0 && !topicAH.isAvailable())
			throw new Exception("Topic has not been specified. Specify either the \"topicAttribute\" or \"topic\" parameters.");
		
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
			"The message is a required attribute. If the key attribute is not specified, the message is used as they key. " +
			"A topic can be specified as either a stream attribute or as a parameter. "
			;
}
