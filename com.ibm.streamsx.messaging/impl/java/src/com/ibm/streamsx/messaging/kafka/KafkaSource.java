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
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;

@OutputPorts(@OutputPortSet(cardinality=1, optional=false, 
			description="Messages received from Kafka are sent on this output port."))
@Libraries({"opt/downloaded/*"})
@PrimitiveOperator(description=KafkaSource.DESC)
public class KafkaSource extends AbstractOperator {
	
	private Properties properties = new Properties(), finalProperties = null;
	private List<String> topics = new ArrayList<String>();
	private String propFile = null;
	private int threadsPerTopic = 1;
	private StreamingOutput<OutputTuple> so = null;
	private AttributeHelper topicAH = new AttributeHelper("topic"),
							keyAH = new AttributeHelper("key"),
							messageAH =  new AttributeHelper("message");
	KafkaClient client= null;
	
	private static Logger trace = Logger.getLogger(KafkaSource.class.getName());
	
	@Override
	public void initialize(OperatorContext context)
			throws Exception {
		super.initialize(context);
		
		finalProperties = new Properties();
		if(propFile != null) {
			finalProperties.load(new FileReader(propFile));
		}
		finalProperties.putAll(properties);
		
		
		if(threadsPerTopic < 1) 
			throw new IllegalArgumentException("Number of threads per topic cannot be less than one: " + threadsPerTopic);
		if(context.getStreamingOutputs().size() != 1) {
			throw new Exception("Exactly one output port supported");
		}
		so = context.getStreamingOutputs().get(0);
		
		topicAH.initialize(so.getStreamSchema(), false);
		keyAH.initialize(so.getStreamSchema(), false);
		messageAH.initialize(so.getStreamSchema(), true);
				
		if(finalProperties == null || finalProperties.isEmpty())
			throw new Exception("Kafka connection properties must be specified.");
		//TODO: check for minimum properties
		trace.log(TraceLevel.INFO, "Initializing Client");
		client = new KafkaClient(topicAH, keyAH, messageAH, finalProperties);
	}
		
	@Override
	public void allPortsReady() throws Exception {
		//initialize the client
		client.initConsumer(getOutput(0), getOperatorContext().getThreadFactory(), topics, threadsPerTopic);
	}
	
	@Parameter(name="kafkaProperty", cardinality=-1, optional=true, 
			description="Specify a Kafka property \\\"key=value\\\" form. This will override any property specified in the properties file.")
	public void setKafkaProperty(List<String> values) {
		for(String value : values) {
			String [] arr = value.split("=");
			if(arr.length < 2) throw new IllegalArgumentException("Invalid property: " + value);
			String name = arr[0];
			String v = value.substring(arr[0].length()+1, value.length());
			properties.setProperty(name, v);
		}
	}
	
	@Parameter(name="topic", cardinality=-1, optional=false, 
			description="Topic to be subscribe to. A topic can also be specified as an input stream attribute.")
	public void setTopic(List<String> values) {
		if(values!=null)
			topics.addAll(values);
	}	
	
	@Parameter(name="propertiesFile", optional=true,
			description="Properties file containing kafka properties.")
	public void setPropertiesFile(String value) {
		this.propFile = value;
	}	
	
	@Parameter(name="threadsPerTopic", optional=true, 
			description="Number of threads per topic. Default is 1.")
	public void setThreads(int value) {
		this.threadsPerTopic = value;
	}	
	
	@Parameter(optional=true, description="Name of the attribute containing the topic. This attribute is optional. Default is \\\"topic\\\"")
	public void setTopicAttribute(String value) {
		topicAH.setName(value);
	}
	@Parameter(optional=true, 
			description="Name of the attribute containing the message. " +
					"This attribute is required to be present in the output stream. Default is \\\"message\\\"")
	public void setMessageAttribute(String value) {
		messageAH.setName (value);
	}
	@Parameter(optional=true, description="Name of the attribute containing the key. This attribute is optional. Default is \\\"key\\\"")
	public void setKeyAttribute(String value) {
		keyAH.setName (value);
	}
	
	@Override
	public void shutdown() {
		client.shutdown();
	}

	public static final String DESC = 
			"This operator acts as a Kafka consumer recieving messages for one or more topics. " +
		    "Note that there may be multiple threads receiving messages depending on the configuration specified. " +
		    "Ordering of messages is not guaranteed."
			;

}

