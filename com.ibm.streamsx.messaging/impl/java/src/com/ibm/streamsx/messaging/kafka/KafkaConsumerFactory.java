package com.ibm.streamsx.messaging.kafka;

import java.util.Properties;
import java.util.logging.Logger;

import com.ibm.streams.operator.logging.TraceLevel;

public class KafkaConsumerFactory {
	KafkaConsumerClient client;
	private final Logger trace = Logger.getLogger(KafkaConsumerFactory.class
			.getCanonicalName());
	
	public KafkaConsumerClient getClient(AttributeHelper topicAH,
			AttributeHelper keyAH, AttributeHelper messageAH, Properties props) {

		if (props.containsKey("bootstrap.servers")){
			if (messageAH.isString()){
				trace.log(TraceLevel.INFO, "Using new 0.9 String consumer client.");
				client = new KafkaStringConsumerV9(topicAH, keyAH, messageAH, props);
			} else {
				trace.log(TraceLevel.INFO, "Using new 0.9 ByteArray consumer client.");
				client = new KafkaByteArrayConsumerV9(topicAH, keyAH, messageAH, props);
			}
		} else {
			trace.log(TraceLevel.INFO, "Using High Level consumer client.");
			client = new KafkaHighLevelConsumer(topicAH, keyAH, messageAH, props);
		}

		return client;
	}
}
