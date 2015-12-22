package com.ibm.streamsx.messaging.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Logger;

import org.apache.kafka.common.TopicPartition;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.state.ConsistentRegionContext;

public class KafkaHighLevelConsumer extends KafkaConsumerClient{
	private  ConsumerConnector consumer;
	private boolean shutdown = false;
	
	public KafkaHighLevelConsumer(AttributeHelper topicAH,
			AttributeHelper keyAH, AttributeHelper messageAH, Properties props) {
		super(topicAH, keyAH, messageAH, props);
	}

	@Override
	protected
	void init(StreamingOutput<OutputTuple> so, ThreadFactory tf,
			List<String> topics) {
		int threadsPerTopic = 1;
		this.streamingOutput = so;
		trace.log(TraceLevel.INFO, "Initializing Kafka consumer: " + props.toString());
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector( new ConsumerConfig(props) );
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
				Thread th = tf.newThread((new HighLevelConsumer(topic, stream, this, threadNumber++)));
				th.setDaemon(false);
				th.start();
			}
		}	
		
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

	@Override
	void shutdown() {
		shutdown = true;
		if(consumer != null) {
			consumer.shutdown();
		}		
	}
	
	public boolean isShutdown() {
		return shutdown;
	}
	
	/**
	 * Kafka Consumer Thread
	 *
	 */
	private static class HighLevelConsumer implements Runnable {
		private String topic = null;
		private KafkaStream<byte[], byte[]> kafkaStream;
		private KafkaHighLevelConsumer client;
		private String baseMsg = null;
		private static final Logger logger = Logger.getLogger(HighLevelConsumer.class.getName());

		public HighLevelConsumer(String topic, KafkaStream<byte[], byte[]> kafkaStream, KafkaHighLevelConsumer kSource, int threadNumber) {
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

	@Override
	protected void setConsistentRegionContext(ConsistentRegionContext crContext, int trigCount) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected Map<Integer, Long> getOffsetPositions()
			throws InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	protected void seekToPositions(Map<Integer, Long> offsetMap) {
		// TODO Auto-generated method stub
		
	}	
}
