/*******************************************************************************
 * Copyright (C) 2016, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.KafkaMetric;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.logging.TraceLevel;

import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.KafkaException;

/* This class provides an implementation of the Kafka 0.9 KafkaConsumer client. It is parametrized with 
 * the expectation of being extended by a <String,String> consumer and a <Byte[],Byte[]> consumer.*/
public abstract class KafkaConsumerClient<K,V> extends KafkaBaseClient {
	protected KafkaConsumer<K,V> consumer;
	private List<Integer> partitions;
	
	private final AtomicBoolean shutdown = new AtomicBoolean(false);

	StreamingOutput<OutputTuple> streamingOutput;
	
	public KafkaConsumerClient(AttributeHelper topicAH, AttributeHelper keyAH, AttributeHelper messageAH, List<Integer> partitions, Properties props) {
		super(topicAH,keyAH,messageAH,props);
		consumer = new KafkaConsumer<K, V>(props); 	
		this.partitions = partitions;
	}
	

	
	protected void init(
			StreamingOutput<OutputTuple> so, List<String> topics){
		streamingOutput = so;
		
		if (partitions == null || partitions.isEmpty()){
			//subscribe to the topics
			trace.log(TraceLevel.INFO, "Subscribing to topics: " + topics.toString()); //$NON-NLS-1$
			consumer.subscribe(topics); 
		} else {
			//subscribe to specific partitions
			List<TopicPartition> partitionList = new ArrayList<TopicPartition>();
			for (Integer partition : partitions){
				TopicPartition tPartition = new TopicPartition(topics.get(0), partition);
				partitionList.add(tPartition);
			}
			trace.log(TraceLevel.INFO, "Subscribing to partitions: " + partitionList.toString() + " in topic: " + topics.get(0)); //$NON-NLS-1$ //$NON-NLS-2$
			consumer.assign(partitionList);
		}	
		
//		processThread = tf.newThread(new Runnable() {
//
//			@Override
//			public void run() {
//				produceTuples();
//			}
//
//		});
//		
//		/*
//		 * Set the thread not to be a daemon to ensure that the SPL runtime will
//		 * wait for the thread to complete before determining the operator is
//		 * complete.
//		 */
//		processThread.setDaemon(false);
//		processThread.start();
	}
	
	protected void wakeupConsumer(){
		consumer.wakeup();
	}
	
	public ConsumerRecords<K, V> getRecords(long consumerPollTimeout) throws InvalidOffsetException 
		, WakeupException, AuthorizationException, KafkaException{
		ConsumerRecords<K,V> records = consumer.poll(consumerPollTimeout);
		return records;
	}
		
	protected synchronized Map<Integer, Long> getOffsetPositions() throws InterruptedException{
		Set<TopicPartition> partitionSet = consumer.assignment();
		Iterator<TopicPartition> partitionIterator = partitionSet.iterator();
		Map<Integer, Long> offsetMap = new HashMap<Integer, Long>();
		while(partitionIterator.hasNext()){
			TopicPartition partition = partitionIterator.next();		
			Long offset = consumer.position(partition);
			offsetMap.put(partition.partition(), offset);
			if(trace.isLoggable(TraceLevel.INFO))
				trace.log(TraceLevel.INFO, "Retrieving offset: " + offset + " for topic: " + partition.topic()); //$NON-NLS-1$ //$NON-NLS-2$
		}
		
		return offsetMap;
	}
	
	protected synchronized void seekToPositions(Map<Integer, Long> offsetMap){
		Set<TopicPartition> partitionSet = consumer.assignment();
		String topic = partitionSet.iterator().next().topic();
		
		Iterator<Entry<Integer, Long>> partitionOffsetIterator = offsetMap.entrySet().iterator();
		while (partitionOffsetIterator.hasNext()){
			Entry<Integer, Long> entry = partitionOffsetIterator.next();
			TopicPartition partition = new TopicPartition(topic, entry.getKey());
			Long offset = entry.getValue();
			trace.log(TraceLevel.INFO, "Seeking to offset: " + offset + " for topic: " + partition.topic() + " from postion: " + consumer.position(partition)); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			consumer.seek(partition, offset);
		}
	}

	public void checkConnectionCount() throws NoKafkaBrokerConnectionsException {
		@SuppressWarnings("unchecked")
		Map<MetricName,KafkaMetric> metricsMap = (Map<MetricName, KafkaMetric>) consumer.metrics();
		
		for (Map.Entry<MetricName,KafkaMetric> metric : metricsMap.entrySet()){
			if (metric.getKey().name().equals("connection-count")){ //$NON-NLS-1$
				if (metric.getValue().value() == 0){
					throw new NoKafkaBrokerConnectionsException();
				}
			}
		}
	}
	
	public void processAndSubmit(ConsumerRecords<K,V> records) throws Exception {
		String topic;
		for (ConsumerRecord<K,V> record : records){
			topic = record.topic();
			if(shutdown.get()) return;
			if(trace.isLoggable(TraceLevel.DEBUG))
				trace.log(TraceLevel.DEBUG, "Topic: " + topic + ", Message: " + record.value() ); //$NON-NLS-1$ //$NON-NLS-2$
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
		trace.log(TraceLevel.ALL, "Shutting down Kafka Consumer Client..."); //$NON-NLS-1$
		shutdown.set(true);
		consumer.close();	
	}
	

}
