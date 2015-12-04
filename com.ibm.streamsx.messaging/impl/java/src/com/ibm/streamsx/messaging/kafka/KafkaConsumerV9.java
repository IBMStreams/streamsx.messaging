package com.ibm.streamsx.messaging.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.logging.TraceLevel;

public abstract class KafkaConsumerV9<K,V> extends KafkaConsumerClient {
	KafkaConsumer<K,V> consumer;
	private int partition = -1;
	static Boolean shutdown = false;
	
	
	public KafkaConsumerV9(AttributeHelper topicAH, AttributeHelper keyAH, AttributeHelper messageAH, Properties props) {
		super(topicAH,keyAH,messageAH,props);
		setDefaultDeserializers();
		consumer = new KafkaConsumer<K, V>(props); 		
	}
	
	private void setDefaultDeserializers() {
		if (!props.containsKey("key.deserializer")){
			if(messageAH.isString()){
				props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
				trace.log(TraceLevel.INFO, "Adding unspecified property key.serializer=org.apache.kafka.common.serialization.StringDeserializer" );
			}
			else{
				props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
				trace.log(TraceLevel.INFO, "Adding unspecified property key.serializer=org.apache.kafka.common.serialization.ByteArrayDeserializer" );
			}
		}
		
		if (!props.containsKey("value.deserializer")){
			if(messageAH.isString()){
				props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
				trace.log(TraceLevel.INFO, "Adding unspecified property value.serializer=org.apache.kafka.common.serialization.StringDeserializer" );
			}
			else{
				props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
				trace.log(TraceLevel.INFO, "Adding unspecified property value.serializer=org.apache.kafka.common.serialization.ByteArrayDeserializer" );
			}
		}
		
	}
	
	public void init(
			StreamingOutput<OutputTuple> so,
			ThreadFactory tf, List<String> topics, int threadsPerTopic){
		streamingOutput = so;
		
		if (partition  == -1){
			//subscribe to the topics
			trace.log(TraceLevel.INFO, "Subscribing to topics: " + topics.toString());
			try {
				consumer.subscribe(topics); 
			} catch (Exception e){
				e.printStackTrace();
				trace.log(TraceLevel.ERROR,"Failed to subscribe. Topics: " + topics.toString() + " consumer: " + consumer.toString());
			}
		} else {
			//subscribe to specific partition
			TopicPartition partition1 = new TopicPartition(topics.get(0), partition);
			List<TopicPartition> partitionList = new ArrayList<TopicPartition>();
			partitionList.add(partition1);
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
		while (!shutdown) {
			try {
				ConsumerRecords<K,V> records = consumer.poll(100);
				process(records);
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
			if(shutdown) return;
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
		shutdown = true;
		if (consumer != null){
			consumer.close();
		}
	}
}
