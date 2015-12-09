package com.ibm.streamsx.messaging.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.logging.TraceLevel;

public abstract class KafkaConsumerV9<K,V> extends KafkaConsumerClient {
	protected KafkaConsumer<K,V> consumer;
	private List<Integer> partitions;
	
	private final AtomicBoolean shutdown = new AtomicBoolean(false);
	private long consumerPollTimeout;
	
	
	public KafkaConsumerV9(AttributeHelper topicAH, AttributeHelper keyAH, AttributeHelper messageAH, List<Integer> partitions, int consumerPollTimeout, Properties props) {
		super(topicAH,keyAH,messageAH,props);
		props = KafkaConfigUtilities.setDefaultDeserializers(keyAH, messageAH, props);
		consumer = new KafkaConsumer<K, V>(props); 	
		this.partitions = partitions;
		this.consumerPollTimeout = consumerPollTimeout;
	}
	

	
	public void init(
			StreamingOutput<OutputTuple> so,
			ThreadFactory tf, List<String> topics, int threadsPerTopic){
		streamingOutput = so;
		
		if (partitions == null || partitions.isEmpty()){
			//subscribe to the topics
			trace.log(TraceLevel.INFO, "Subscribing to topics: " + topics.toString());
			try {
				consumer.subscribe(topics); 
			} catch (Exception e){
				e.printStackTrace();
				trace.log(TraceLevel.ERROR,"Failed to subscribe. Topics: " + topics.toString() + " consumer: " + consumer.toString());
			}
		} else {
			//subscribe to specific partitions
			List<TopicPartition> partitionList = new ArrayList<TopicPartition>();
			for (Integer partition : partitions){
				TopicPartition tPartition = new TopicPartition(topics.get(0), partition);
				partitionList.add(tPartition);
			}
			trace.log(TraceLevel.INFO, "Subscribing to partitions: " + partitionList.toString() + " in topic: " + topics.get(0));
			try {
				consumer.assign(partitionList);
			} catch (Exception e){
				e.printStackTrace();
				trace.log(TraceLevel.ERROR,"Failed to subscribe to specified partitions. Make sure they exist.");
			}
		}
			
		
		processThread = tf.newThread(new Runnable() {

			@Override
			public void run() {
				try {
					produceTuples();
				} catch (Exception e) {
					trace.log(TraceLevel.ERROR, "Operator error: " + e.getMessage() + "\n" + e.getStackTrace());
				}
			}

		});
		
		/*
		 * Set the thread not to be a daemon to ensure that the SPL runtime will
		 * wait for the thread to complete before determining the operator is
		 * complete.
		 */
		processThread.setDaemon(false);
		processThread.start();
	}
	
	public void produceTuples(){
		while (!shutdown.get()) {
			try {
				ConsumerRecords<K,V> records = consumer.poll(consumerPollTimeout);
				process(records);
			} catch (WakeupException e) {
	             // Ignore exception if closing
	             if (!shutdown.get()){
	            	 trace.log(TraceLevel.ERROR, "WakeupException: " + e.getMessage());
	            	 e.printStackTrace();
	             }
	        } catch (Exception e) {
				trace.log(TraceLevel.ERROR, "Error processing messages: " + e.getMessage());
				e.printStackTrace();
			}
		}
	}
	
	private void process(ConsumerRecords<K,V> records) throws Exception {
		String topic;
		
		for (ConsumerRecord<K,V> record : records){
			topic = record.topic();
			if(shutdown.get()) return;
			if(trace.isLoggable(TraceLevel.DEBUG))
				trace.log(TraceLevel.DEBUG, "Topic: " + topic + ", Message: " + record.value() );
			OutputTuple otup = streamingOutput.newTuple();
			if(topicAH.isAvailable())
				topicAH.setValue(otup, topic);
			if(keyAH.isAvailable())
				setKeyValue(keyAH, otup, record.key());
				
			setMessageValue(messageAH, otup, record.value());
			streamingOutput.submit(otup);
		}
	}
	
	protected abstract void setMessageValue(AttributeHelper messageAH, OutputTuple otup,
			V message) ;

	protected abstract void setKeyValue(AttributeHelper keyAH, OutputTuple otup,
			K key);
	
	public void shutdown() {
		System.out.println("Shutting down");
		shutdown.set(true);
		if (consumer != null){
			consumer.close();
		}
	}
}
