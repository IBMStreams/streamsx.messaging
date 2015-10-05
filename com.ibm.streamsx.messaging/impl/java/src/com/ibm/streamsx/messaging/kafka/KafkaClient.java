/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.messaging.kafka;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.logging.TraceLevel;

class KafkaClient {
	
	
	static final Charset CS = Charset.forName("UTF-8");
	
	private AttributeHelper topicAH = null, keyAH = null, messageAH = null;
	private StreamingOutput<OutputTuple> streamingOutput = null;
	
	private  ConsumerConnector consumer;
	private boolean shutdown  = false;
	private Properties finalProperties = null;
	private boolean isInit = false, isConsumer = false;
	
	private AProducerHelper producer = null;
	

	static final Logger trace = Logger.getLogger(KafkaClient.class.getCanonicalName());
	
	public KafkaClient(AttributeHelper topicAH, AttributeHelper keyAH, AttributeHelper 	messageAH, Properties props) {
		this.topicAH = topicAH;
		this.keyAH = keyAH;
		this.messageAH =  messageAH;
		this.finalProperties = props;
	}
	
	private synchronized void checkInit(boolean isCons) {
		if(isInit)
			throw new RuntimeException("Client has already been initialized. Cannot initialized again");
		isConsumer = isCons;
		isInit = true;
	}
	
	//producer related methods
	public void initProducer() throws Exception {
		checkInit(false);
		
		trace.log(TraceLevel.INFO, "Initializing Kafka Producer: " + finalProperties);
		
		setDefaultSerializers(finalProperties);
		//handle default key.serializer
		

		if(messageAH.isString())
			producer = new ProducerStringHelper();
		else
			producer = new ProducerByteHelper();
		
		producer.init(finalProperties, keyAH, messageAH);
	}
	
	private void setDefaultSerializers(Properties finalProperties2) {
		if (!finalProperties.containsKey("key.serializer")){
			if(messageAH.isString()){
				finalProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			}
			else{
				finalProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
			}
		}
		
		if (!finalProperties.containsKey("value.serializer")){
			if(messageAH.isString()){
				finalProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			}
			else{
				finalProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
			}
		}
		
	}

	public void send(Tuple tuple, List<String> topics) throws Exception {
		producer.send(tuple, topics);
	}
	
	public void send(Tuple tuple) throws Exception {		
		producer.send(tuple,  topicAH);
	}
	
	
	//Consumer related methods
	
	public void initConsumer(
			StreamingOutput<OutputTuple> so,
			ThreadFactory tf, List<String> topics, int threadsPerTopic) {
		checkInit(true);
		
		this.streamingOutput = so;
		trace.log(TraceLevel.INFO, "Initializing Kafka consumer: " + finalProperties.toString());
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector( new ConsumerConfig(finalProperties) );
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		for(String topic : topics) {
			topicCountMap.put(topic, threadsPerTopic);        
		}
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		int threadNumber = 0;
		for(String topic : topics) {
			List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);        // now launch all the threads
			for (KafkaStream<byte[], byte[]> stream : streams) {
				trace.log(TraceLevel.INFO, "Starting thread [" + threadNumber + "] for topic: " + topic);
				Thread th = tf.newThread((new KafkaConsumer(topic, stream, this, threadNumber++)));
				th.setDaemon(false);
				th.start();
			}
		}		
	}
	
	public void shutdown() {
		shutdown = true;
		if(isConsumer) {
			consumer.shutdown();
		}
	}

	public boolean isShutdown() {
		return shutdown;
	}
	
	private void newMessage(String topic, MessageAndMetadata<byte[], byte[]> msg) throws Exception {
		if(shutdown) return;
		if(trace.isLoggable(TraceLevel.DEBUG))
			trace.log(TraceLevel.DEBUG, "Topic: " + topic + ", Message: " + msg );
		OutputTuple otup = streamingOutput.newTuple();
		if(topicAH.isAvailable())
			topicAH.setValue(otup, topic);
		if(keyAH.isAvailable())
			keyAH.setValue(otup, msg.key());
		messageAH.setValue(otup, msg.message());
		streamingOutput.submit(otup);
	}

	
	/**
	 * Kafka Consumer Thread
	 *
	 */
	private static class KafkaConsumer implements Runnable {
		private String topic = null;
		private KafkaStream<byte[], byte[]> kafkaStream;
		private KafkaClient client;
		private String baseMsg = null;
		private static final Logger logger = Logger.getLogger(KafkaConsumer.class.getName());

		public KafkaConsumer(String topic, KafkaStream<byte[], byte[]> kafkaStream, KafkaClient kSource, int threadNumber) {
			this.topic = topic;
			this.kafkaStream = kafkaStream;
			this.client = kSource;
			baseMsg = " Topic[" + topic + "] Thread[" + threadNumber +"] ";
		}

		public void run() {
			logger.log(TraceLevel.INFO,baseMsg + "Thread Started");
			for(MessageAndMetadata<byte[], byte[]> ret : kafkaStream) {
				if(client.isShutdown()) break;
				if(logger.isLoggable(TraceLevel.DEBUG))
					logger.log(TraceLevel.DEBUG, baseMsg + "New Message");
				try {
					client.newMessage(topic, ret);
				} catch (Exception e) {
					if(logger.isLoggable(TraceLevel.ERROR))
						logger.log(TraceLevel.ERROR, baseMsg + "Could not send message", e);
				}
			}
			logger.log(TraceLevel.INFO,baseMsg + "Thread Stopping");
		}
	}
}

abstract class AProducerHelper {
	AttributeHelper keyAH=null, messageAH = null;

	abstract void init(Properties finalProperties, AttributeHelper keyAH,
			AttributeHelper messageAH) throws Exception;
	
	abstract void send(Tuple tuple, AttributeHelper topicAH)  throws Exception;
	
	abstract void send(Tuple tuple,  List<String> topics)  throws Exception;
	
} 

class ProducerStringHelper extends AProducerHelper{
	AttributeHelper keyAH=null, messageAH = null;
	private KafkaProducer<String, String> producer = null;

	@Override
	void init(Properties finalProperties, AttributeHelper keyAH,
			AttributeHelper messageAH) {
		producer = new KafkaProducer<String, String>(finalProperties);
		this.keyAH = keyAH;
		this.messageAH = messageAH;
	}

	@Override
	void send(Tuple tuple, AttributeHelper topicAH) throws Exception {
		String topic = topicAH.getString(tuple);
		String message = messageAH.getString(tuple);
		String key = keyAH.getString(tuple);
		if (key == null) {
			key = message;
		}
		producer.send(new ProducerRecord<String, String>(topic ,key, message));
	}

	@Override
	void send(Tuple tuple, List<String> topics) throws Exception {
		String message = messageAH.getString(tuple);
		String key = keyAH.getString(tuple);
		if (key == null) {
			key = message;
		}
		for(String topic : topics) {
			producer.send(new ProducerRecord<String, String>(topic,key, message));
		}

	}
}

class ProducerByteHelper extends AProducerHelper{
	AttributeHelper keyAH=null, messageAH = null;
	private KafkaProducer<byte[],byte[]> producer = null;

	@Override
	void init(Properties finalProperties, AttributeHelper keyAH,
			AttributeHelper messageAH) {
		producer = new KafkaProducer<byte[],byte[]>(finalProperties);
		this.keyAH = keyAH;
		this.messageAH = messageAH;
	}

	@Override
	void send(Tuple tuple, AttributeHelper topicAH) throws Exception {
		String topic = topicAH.getString(tuple);
		byte [] message = messageAH.getBytes(tuple);
		byte [] key = keyAH.getBytes(tuple);
		if (key == null) {
			key = message;
		}
		producer.send(new ProducerRecord<byte[],byte[]>(topic ,key, message));
	}

	@Override
	void send(Tuple tuple, List<String> topics) throws Exception {
		byte [] message = messageAH.getBytes(tuple);
		byte [] key = keyAH.getBytes(tuple);
		if (key == null) {
			key = message;
		}
		for(String topic : topics) {
			producer.send(new ProducerRecord<byte[],byte[]>(topic,key, message));
		}

	}
}

