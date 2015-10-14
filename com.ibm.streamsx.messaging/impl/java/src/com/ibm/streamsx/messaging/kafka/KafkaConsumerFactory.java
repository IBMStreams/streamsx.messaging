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

		if (props.containsKey("security.protocol")
				&& props.getProperty("security.protocol").equalsIgnoreCase("SSL")){
			trace.log(TraceLevel.WARNING, "Using new 0.9 consumer client.");
			client = new StreamsKafkaConsumer9(topicAH, keyAH, messageAH, props);
		} else {
			trace.log(TraceLevel.WARNING, "Using High Level consumer client.");
			client = new KafkaHighLevelConsumer(topicAH, keyAH, messageAH, props);
		}

		return client;
	}
}
