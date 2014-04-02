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

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;

@InputPorts(@InputPortSet(cardinality=1, optional=false))
@OutputPorts(@OutputPortSet(cardinality=1, optional=false))
@PrimitiveOperator(description=KafkaSink.DESC)
@Libraries({"@KAFKA_HOME@/*", "@KAFKA_HOME@/libs/*"})
public class KafkaSink extends AbstractOperator {
	private Properties properties = new Properties(), finalProperties = null;
	private List<String> topics = new ArrayList<String>();
	private String propFile = null;
	private Producer<String, String> producer = null;
	
	private AttributeHelper topicAH = new AttributeHelper("topic"),
							keyAH = new AttributeHelper("key"),
							messageAH = new AttributeHelper("message");
	
	private static Logger trace = Logger.getLogger(KafkaSink.class.getName());

	
	
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
		topicAH.wasSet(true);
		topicAH.setName(value);
	}
	@Parameter(optional=true, description="Name of the attribute containing the message. Default is \\\"message\\\"")
	public void setMessageAttribute(String value) {
		messageAH.wasSet(true);
		messageAH.setName (value);
	}
	@Parameter(optional=true, description="Name of the attribute containing the key. Default is \\\"key\\\"")
	public void setKeyAttribute(String value) {
		keyAH.wasSet (true);
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
		
		trace.log(TraceLevel.INFO, "Initializing Kafka consumer");
		ProducerConfig config = new ProducerConfig(finalProperties);
		producer = new Producer<String, String>(config);
	}
	
	
	@Override
	public void process(StreamingInput<Tuple> stream, Tuple tuple)
			throws Exception {
		try {
		
			if(trace.isLoggable(TraceLevel.INFO))
				trace.log(TraceLevel.INFO, "Sending message: " + tuple.toString());

			String data = messageAH.getValue(tuple);
			String key = data;
			if(keyAH.isAvailable()) {
				key=keyAH.getValue(tuple);
			}
			
			if(!topics.isEmpty()) {
				List<KeyedMessage<String, String> > lst = new ArrayList<KeyedMessage<String,String>>();
				for(String topic : topics) {
					lst.add(new KeyedMessage<String, String>(topic, key, data));
				}
				producer.send(lst);
			}
			else {
				String topic = topicAH.getValue(tuple); 
				KeyedMessage<String, String> keyedMessage = new KeyedMessage<String, String>(topic, key, data);
				producer.send(keyedMessage);
			}
		}catch(Exception e) {
			trace.log(TraceLevel.ERROR, "Could not send message: " + tuple.toString(), e);
		}
	}	
	
	public static final String DESC = 
			"This operator acts as a producer sending tuples as mesages to a Kafka broker." + 
			"The broker is assumed to be already configured and running. " +
			"If a \\\"key\\\" attribute is not specified, the entire message is used as the key. " +
			"A topic can be specified as either a stream attribute or as a parameter. "
			;
}
