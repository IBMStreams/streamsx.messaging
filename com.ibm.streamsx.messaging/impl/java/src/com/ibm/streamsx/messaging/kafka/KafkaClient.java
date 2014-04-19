package com.ibm.streamsx.messaging.kafka;

import java.nio.charset.Charset;
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
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.logging.TraceLevel;

class KafkaClient {
	
	
	static final Charset CS = Charset.forName("UTF-8");
	
	KafkaSource source = null;
	AttributeHelper topicAH = null, keyAH = null, messageAH = null;
	StreamingOutput<OutputTuple> streamingOutput = null;
	
	private  ConsumerConnector consumer;
	boolean shutdown  = false;
	Properties finalProperties = null;
	boolean isInit = true, isConsumer = false;
	
	private Producer<byte[],byte[]> producer = null;

	static Logger trace = Logger.getLogger(KafkaClient.class.getCanonicalName());
	
	public KafkaClient(AttributeHelper topicAH, AttributeHelper keyAH, AttributeHelper 	messageAH, Properties props) {
		this.topicAH = topicAH;
		this.keyAH = keyAH;
		this.messageAH =  messageAH;
		this.finalProperties = props;
	}
	
	private synchronized void checkInit(boolean isCons) {
		if(isInit)
			throw new RuntimeException("Client has already been initialized. Cannot initialized again");
		isInit = true;
		isConsumer = isCons;
	}
	
	//producer related methods
	public synchronized void initProducer() {
		checkInit(false);
		
		trace.log(TraceLevel.INFO, "Initializing Kafka Producer: " + finalProperties);
		ProducerConfig config = new ProducerConfig(finalProperties);
		producer = new Producer<byte[], byte[]>(config);
	}
	
	public void send(Tuple tuple, List<String> topics) {
		String data = messageAH.getValue(tuple);
		String key = data;
		if(keyAH.isAvailable()) {
			key=keyAH.getValue(tuple);
		}

		List<KeyedMessage<byte[], byte[]> > lst = new ArrayList<KeyedMessage<byte[],byte[]>>();
		for(String topic : topics) {
			lst.add(new KeyedMessage<byte[], byte[]>(topic, key.getBytes(CS), data.getBytes(CS)));
		}
		producer.send(lst);
	}
	public void send(Tuple tuple) {
		String data = messageAH.getValue(tuple);
		String key = data;
		if(keyAH.isAvailable()) {
			key=keyAH.getValue(tuple);
		}

		String topic = topicAH.getValue(tuple); 	
		KeyedMessage<byte[], byte[]> keyedMessage = new KeyedMessage<byte[], byte[]>(topic,key.getBytes(CS), data.getBytes(CS));
		producer.send(keyedMessage);
	}
	
	
	//Consumer related methods
	
	public synchronized void initConsumer(
			StreamingOutput<OutputTuple> so,
			ThreadFactory tf, List<String> topics, int threadsPerTopic) {
		checkInit(true);
		
		this.streamingOutput = so;
		trace.log(TraceLevel.INFO, "Initializing Kafka consumer: " + finalProperties.toString());
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector( new ConsumerConfig(finalProperties) );
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		for(String topic : topics) {
			trace.log(TraceLevel.INFO, "Starting threads for topic: " + topic);
			topicCountMap.put(topic, new Integer(threadsPerTopic));        
		}
		int threadNumber = 0;
		for(String topic : topics) {
			Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
			List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);        // now launch all the threads
			for (KafkaStream<byte[], byte[]> stream : streams) {
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
		OutputTuple otup = streamingOutput.newTuple();
		topicAH.setValue(otup, topic);
		keyAH.setValue(otup, msg.key());
		messageAH.setValue(otup, msg.message());
		streamingOutput.submit(otup);
	}

	
	private static class KafkaConsumer implements Runnable {
		private String topic = null;
		private KafkaStream<byte[], byte[]> kafkaStream;
		private KafkaClient kSource;
		private String baseMsg = null;
		private static Logger logger = Logger.getLogger(KafkaConsumer.class.getName());

		public KafkaConsumer(String topic, KafkaStream<byte[], byte[]> kafkaStream, KafkaClient kSource, int threadNumber) {
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
	
	
}
