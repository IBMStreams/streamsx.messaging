//
// *******************************************************************************
// * Copyright (C)2014, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
//
package com.ibm.streamsx.messaging.kafka;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;

@InputPorts(@InputPortSet(cardinality=1, optional=false, 
	description="The tuples arriving on this port are expected to contain three attributes \\\"key\\\", \\\"topic\\\" and \\\"message\\\". " +
			"Out of these \\\"message\\\", is a required attribute."))
@PrimitiveOperator(description=KafkaSink.DESC)
@Libraries({"opt/downloaded/*"})
public class KafkaSink extends AbstractOperator {
	private Properties properties = new Properties(), finalProperties = null;
	private List<String> topics = new ArrayList<String>();
	private String propFile = null;
	
	private AttributeHelper topicAH = new AttributeHelper("topic"),
							keyAH = new AttributeHelper("key"),
							messageAH = new AttributeHelper("message");
	
	private static Logger trace = Logger.getLogger(KafkaSink.class.getName());

	KafkaClient client = null;
	
	@Parameter(name="kafkaProperty", cardinality=-1, optional=true, 
			description="Specify a Kafka property \\\"key=value\\\" form. This will override any property specified in the properties file.")
	public void setProperty(List<String> values) {
		for(String value : values) {
			String [] arr = value.split("=");
			if(arr.length < 2) throw new IllegalArgumentException("Invalid property: " + value);
			String name = arr[0];
			String v = value.substring(arr[0].length()+1, value.length());
			properties.setProperty(name, v);
		}
	}
	
	@Parameter(name="topic", cardinality=-1, optional=true, 
			description="Topic to be published to. A topic can also be specified as an input stream attribute.")
	public void setTopic(List<String> values) {
		if(values!=null)
			topics.addAll(values);
	}	
	
	@Parameter(name="propertiesFile", optional=true,
			description="Properties file containing kafka properties.")
	public void setPropertiesFile(String value) {
		this.propFile = value;
	}	

	@Parameter(optional=true, description="Name of the attribute containing the topic. Default is \\\"topic\\\"")
	public void setTopicAttribute(String value) {
		topicAH.setName(value);
	}
	@Parameter(optional=true, description="Name of the attribute containing the message. Default is \\\"message\\\"")
	public void setMessageAttribute(String value) {
		messageAH.setName (value);
	}
	@Parameter(optional=true, description="Name of the attribute containing the key. Default is \\\"key\\\"")
	public void setKeyAttribute(String value) {
		keyAH.setName (value);
	}

	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
		super.initialize(context);
		
		
		finalProperties = new Properties();
		if(propFile != null) {
			finalProperties.load(new FileReader(propFile));
		}
		finalProperties.putAll(properties);
		
		StreamingInput<Tuple> si = context.getStreamingInputs().get(0);
		
		topicAH.initialize(si.getStreamSchema(), false);
		keyAH.initialize(si.getStreamSchema(), false);
		messageAH.initialize(si.getStreamSchema(), true);
		
				
		if(topics.size() > 0 && topicAH.isAvailable())
			throw new Exception("Topic attribute parameter and topics values have both been set. Please use only one.");
		if(topics.size() == 0 && !topicAH.isAvailable())
			throw new Exception("Topic has not been specified. Specify either the \"topicAttribute\" or \"topic\" parameters.");
		if(finalProperties == null || finalProperties.isEmpty())
			throw new Exception("Kafka connection properties must be specified.");
		//TODO: check for minimum properties
		client = new KafkaClient(topicAH, keyAH, messageAH, finalProperties);
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
			trace.log(TraceLevel.ERROR, "Could not send message: " + tuple.toString(), e);
		}
	}	
	
	public static final String DESC = 
			"This operator acts as a producer sending tuples as mesages to a Kafka broker." + 
			"The broker is assumed to be already configured and running. " +
			"If a \\\"key\\\" attribute is not specified, the message is used as the key. " +
			"A topic can be specified as either a stream attribute or as a parameter. "
			;
}
