//
// *******************************************************************************
// * Copyright (C)2014, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
//
package com.ibm.streamsx.messaging.kafka;


import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Logger;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;

@InputPorts(@InputPortSet(cardinality=0))
@OutputPorts(@OutputPortSet(cardinality=1, optional=false, 
			description="Messages received from Kafka are sent on this output port."))
@Libraries({"@KAFKA_HOME@/*", "@KAFKA_HOME@/libs/*"})
@PrimitiveOperator(description=KafkaSource.DESC)
public class KafkaSource extends AbstractOperator {
	private  ConsumerConnector consumer;
	
	private Properties properties = new Properties(), finalProperties = null;
	private List<String> topics = new ArrayList<String>();
	private String propFile = null;
	private int numThreads = 1;
	private boolean shutdown = false;
	private StreamingOutput<OutputTuple> so = null;
	private AttributeHelper topicAH = new AttributeHelper("topic"),
							keyAH = new AttributeHelper("key"),
							messageAH =  new AttributeHelper("message");
	
	
	private static Logger trace = Logger.getLogger(KafkaSource.class.getName());
	
	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
		super.initialize(context);
		
		finalProperties = new Properties();
		if(propFile != null) {
			finalProperties.load(new FileReader(propFile));
		}
		finalProperties.putAll(properties);
		
		
		if(numThreads < 1) 
			throw new IllegalArgumentException("Number of threads cannot be less than one: " + numThreads);
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
		
		trace.log(TraceLevel.INFO, "Initializing Kafka consumer: " + finalProperties.toString());
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector( new ConsumerConfig(finalProperties) );
	}
		
	@Override
	public synchronized void allPortsReady() throws Exception {
		
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		for(String topic : topics) {
			trace.log(TraceLevel.INFO, "Starting threads for topic: " + topic);
			topicCountMap.put(topic, new Integer(numThreads));        
		}
		int threadNumber = 0;
		for(String topic : topics) {
			Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
			List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);        // now launch all the threads
			ThreadFactory tf = getOperatorContext().getThreadFactory(); 
			for (KafkaStream<byte[], byte[]> stream : streams) {
				Thread th = tf.newThread((new KafkaConsumer(topic, stream, this, threadNumber++)));
				th.setDaemon(false);
				th.start();
			}
		}		
	}
	
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
		this.numThreads = value;
	}	
	
	@Parameter(optional=true, description="Name of the attribute containing the topic. This attribute is optional. Default is \\\"topic\\\"")
	public void setTopicAttribute(String value) {
		topicAH.wasSet(true);
		topicAH.setName(value);
	}
	@Parameter(optional=true, description="Name of the attribute containing the message. This attribute is required to be present in the output stream. Default is \\\"message\\\"")
	public void setMessageAttribute(String value) {
		messageAH.wasSet(true);
		messageAH.setName (value);
	}
	@Parameter(optional=true, description="Name of the attribute containing the key. his attribute is optional. Default is \\\"key\\\"")
	public void setKeyAttribute(String value) {
		keyAH.wasSet (true);
		keyAH.setName (value);
	}
	
	@Override
	public void shutdown() {
		shutdown = true;
		consumer.shutdown();
	}

	private boolean isShutdown() {
		return shutdown;
	}
	
	private synchronized void newMessage(String topic, MessageAndMetadata<byte[], byte[]> msg) throws Exception {
		OutputTuple otup = so.newTuple();
		topicAH.setValue(otup, topic);
		keyAH.setValue(otup, msg.key());
		messageAH.setValue(otup, msg.message());
		so.submit(otup);
	}

	private static class KafkaConsumer implements Runnable {
		private String topic = null;
		private KafkaStream<byte[], byte[]> kafkaStream;
		private KafkaSource kSource;
		private String baseMsg = null;
		private static Logger logger = Logger.getLogger(LoggerNames.LOG_FACILITY + "." + KafkaConsumer.class.getName());

		public KafkaConsumer(String topic, KafkaStream<byte[], byte[]> kafkaStream, KafkaSource kSource, int threadNumber) {
			this.topic = topic;
			this.kafkaStream = kafkaStream;
			this.kSource = kSource;
			baseMsg = " Topic[" + topic + "] Thread[" + threadNumber +"] ";
		}

		public void run() {
			logger.log(TraceLevel.INFO,baseMsg + "Thread Started");
			ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
			while (!kSource.isShutdown() && it.hasNext()) {
				MessageAndMetadata<byte[], byte[]> ret = it.next();
				if(kSource.isShutdown()) break;
				if(logger.isLoggable(TraceLevel.INFO))
					logger.log(TraceLevel.INFO,baseMsg + "New Message");
				try {
					kSource.newMessage(topic, ret);
				} catch (Exception e) {
					if(logger.isLoggable(TraceLevel.ERROR))
						logger.log(TraceLevel.ERROR, baseMsg + "Could not send tuple", e);
				}
			}             
			logger.log(TraceLevel.INFO,baseMsg + "Thread Stopping");
		}
	}
	public static final String DESC = 
			"This operator acts as a Kafka consumer recieving messages for one or more topics. " +
		    "Note that there may be multiple threads receiving messages depending on the configuration specified. " +
		    "Ordering of messages is not guaranteed."
			;

}

