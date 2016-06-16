package com.ibm.streamsx.messaging.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
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
import com.ibm.streams.operator.state.ConsistentRegionContext;

/* This class provides an implementation of the Kafka 0.9 KafkaConsumer client. It is parametrized with 
 * the expectation of being extended by a <String,String> consumer and a <Byte[],Byte[]> consumer.*/
public abstract class KafkaConsumerV9<K,V> extends KafkaConsumerClient {
	protected KafkaConsumer<K,V> consumer;
	private List<Integer> partitions;
	
	private final AtomicBoolean shutdown = new AtomicBoolean(false);
	private long consumerPollTimeout;
	private ConsistentRegionContext crContext; 
	private int triggerCount;
	private long triggerIteration = 0;
	
	public KafkaConsumerV9(AttributeHelper topicAH, AttributeHelper keyAH, AttributeHelper messageAH, List<Integer> partitions, int consumerPollTimeout, Properties props) {
		super(topicAH,keyAH,messageAH,props);
		consumer = new KafkaConsumer<K, V>(props); 	
		this.partitions = partitions;
		this.consumerPollTimeout = consumerPollTimeout;
	}
	

	
	protected void init(
			StreamingOutput<OutputTuple> so,
			ThreadFactory tf, List<String> topics){
		streamingOutput = so;
		
		if (partitions == null || partitions.isEmpty()){
			//subscribe to the topics
			trace.log(TraceLevel.INFO, "Subscribing to topics: " + topics.toString());
			consumer.subscribe(topics); 
		} else {
			//subscribe to specific partitions
			List<TopicPartition> partitionList = new ArrayList<TopicPartition>();
			for (Integer partition : partitions){
				TopicPartition tPartition = new TopicPartition(topics.get(0), partition);
				partitionList.add(tPartition);
			}
			trace.log(TraceLevel.INFO, "Subscribing to partitions: " + partitionList.toString() + " in topic: " + topics.get(0));
			consumer.assign(partitionList);
		}	
		
		processThread = tf.newThread(new Runnable() {

			@Override
			public void run() {
				produceTuples();
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
	
	protected void setConsistentRegionContext(ConsistentRegionContext operatorCrContext, int trigCount){
		crContext = operatorCrContext;
		triggerCount = trigCount;
	}
	
	@Override
	protected synchronized Map<Integer, Long> getOffsetPositions() throws InterruptedException{
		Set<TopicPartition> partitionSet = consumer.assignment();
		Iterator<TopicPartition> partitionIterator = partitionSet.iterator();
		Map<Integer, Long> offsetMap = new HashMap<Integer, Long>();
		while(partitionIterator.hasNext()){
			TopicPartition partition = partitionIterator.next();		
			Long offset = consumer.position(partition);
			offsetMap.put(partition.partition(), offset);
			if(trace.isLoggable(TraceLevel.INFO))
				trace.log(TraceLevel.INFO, "Retrieving offset: " + offset + " for topic: " + partition.topic());
		}
		
		return offsetMap;
	}
	
	@Override
	protected synchronized void seekToPositions(Map<Integer, Long> offsetMap){
		Set<TopicPartition> partitionSet = consumer.assignment();
		String topic = partitionSet.iterator().next().topic();
		
		Iterator<Entry<Integer, Long>> partitionOffsetIterator = offsetMap.entrySet().iterator();
		while (partitionOffsetIterator.hasNext()){
			Entry<Integer, Long> entry = partitionOffsetIterator.next();
			TopicPartition partition = new TopicPartition(topic, entry.getKey());
			Long offset = entry.getValue();
			trace.log(TraceLevel.INFO, "Seeking to offset: " + offset + " for topic: " + partition.topic() + " from postion: " + consumer.position(partition));
			consumer.seek(partition, offset);
		}
	}
	
	
	public void produceTuples(){
		while (!shutdown.get()) {
			try {
				if (crContext != null){
					if(trace.isLoggable(TraceLevel.DEBUG))
						trace.log(TraceLevel.TRACE, "Acquiring consistent region permit.");
					crContext.acquirePermit();
				}
				ConsumerRecords<K,V> records = consumer.poll(consumerPollTimeout);
				process(records);
			} catch (WakeupException e) {
	             // Ignore exception if closing
	             if (shutdown.get()){
	            	 trace.log(TraceLevel.INFO, "Shutting down consumer.");
	            	 consumer.close();
	             } else {
	            	 trace.log(TraceLevel.ERROR, "WakeupException: " + e.getMessage());
	            	 e.printStackTrace();
	             }
	        } catch (Exception e) {
				trace.log(TraceLevel.ERROR, "Error processing messages: " + e.getMessage());
				e.printStackTrace();
			} finally {
				if (crContext != null){
					crContext.releasePermit();
					if(trace.isLoggable(TraceLevel.DEBUG))
						trace.log(TraceLevel.TRACE, "Released consistent region permit.");
				}
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
		
		if (crContext != null
				&& crContext.isTriggerOperator()) {
			triggerIteration += records.count();
			if (triggerIteration >= triggerCount) {
				trace.log(TraceLevel.INFO, "Making consistent..." );
				crContext.makeConsistent();
				triggerIteration = 0;
			}
		}
	}
	
	protected abstract void setMessageValue(AttributeHelper messageAH, OutputTuple otup,
			V message) ;

	protected abstract void setKeyValue(AttributeHelper keyAH, OutputTuple otup,
			K key);
	
	public void shutdown() {
		trace.log(TraceLevel.ALL, "Shutting down...");
		shutdown.set(true);
		if (consumer != null){
			consumer.wakeup();
		}
	}
}
