package com.ibm.streamsx.messaging.kafka;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.logging.TraceLevel;

public class StreamsKafkaConsumer9 extends KafkaConsumerClient {
	KafkaConsumer<String, String> consumer;
	static Boolean shutdown = false;
	
	
	public StreamsKafkaConsumer9(AttributeHelper topicAH, AttributeHelper keyAH, AttributeHelper messageAH, Properties props) {
		this.topicAH = topicAH;
		this.keyAH = keyAH;
		this.messageAH =  messageAH;
		this.props = props;
		
		consumer = new KafkaConsumer<String,String>(props); 
	}
	
	public void init(
			StreamingOutput<OutputTuple> so,
			ThreadFactory tf, List<String> topics, int threadsPerTopic){
		streamingOutput = so;
		
		try {
			consumer.subscribe(topics); 
		} catch (Exception e){
			System.out.println("Failed to subscribe. Topics: " + topics.toString() + " consumer: " + consumer.toString());
		}
		
		processThread = tf.newThread(new Runnable() {

			@Override
			public void run() {
				try {
					produceTuples();
				} catch (Exception e) {
//					TRACE.log(TraceLevel.ERROR, "Operator error: " + e.getMessage() + "\n" + e.getStackTrace());
//					Logger.getLogger(this.getClass())
//							.error("Operator error", e); //$NON-NLS-1$
					System.out.println("Catching produceTuples return");
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
				ConsumerRecords<String,String> records = consumer.poll(100);
				process(records);
			} catch (Exception e) {
				System.out.println("Closing from catch: " + e);
				break;
			}
		}
	}
	
	private void process(ConsumerRecords<String, String> records) throws Exception {
		String topic;
		
		for (ConsumerRecord<String, String> record : records){
			topic = record.topic();
			if(shutdown) return;
			if(trace.isLoggable(TraceLevel.DEBUG))
				trace.log(TraceLevel.DEBUG, "Topic: " + topic + ", Message: " + record.value() );
			OutputTuple otup = streamingOutput.newTuple();
			if(topicAH.isAvailable())
				topicAH.setValue(otup, topic);
			if(keyAH.isAvailable())
				keyAH.setValue(otup, record.key());
			messageAH.setValue(otup, record.value());
			streamingOutput.submit(otup);
		}
	}
	
	public void shutdown() {
		System.out.println("Shutting down");
		shutdown = true;
		if (consumer != null){
			consumer.close();
		}
	}
}
