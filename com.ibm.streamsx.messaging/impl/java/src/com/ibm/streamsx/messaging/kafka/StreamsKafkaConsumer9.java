package com.ibm.streamsx.messaging.kafka;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingOutput;

public class StreamsKafkaConsumer9 extends KafkaConsumerClient {
	KafkaConsumer<String, String> consumer;
	static Boolean shutdown = false;
	StreamingOutput<OutputTuple> oTuple;
	
	public StreamsKafkaConsumer9(AttributeHelper topicAH, AttributeHelper keyAH, AttributeHelper 	messageAH, Properties props) {
		this.topicAH = topicAH;
		this.keyAH = keyAH;
		this.messageAH =  messageAH;
		this.props = props;
	}
	
	public void init(
			StreamingOutput<OutputTuple> so,
			ThreadFactory tf, List<String> topics, int threadsPerTopic){
		oTuple = so;
		
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
	
	private static void process(ConsumerRecords<String, String> records) {
		for (ConsumerRecord<String, String> record : records){
			System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
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
