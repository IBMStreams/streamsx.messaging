package com.ibm.streamsx.messaging.kafka;

import java.util.Properties;
import java.util.logging.Logger;

import com.ibm.streams.operator.logging.TraceLevel;

public class KafkaClientFactory {
	KafkaConsumerClient client;
	private final Logger trace = Logger.getLogger(KafkaClientFactory.class
			.getCanonicalName());
	
	public KafkaConsumerClient getClient(AttributeHelper topicAH,
			AttributeHelper keyAH, AttributeHelper messageAH, Properties props) {

		trace.log(TraceLevel.INFO, "Using new consumer client.");
		client = new StreamsKafkaConsumer9(topicAH, keyAH, messageAH, props);

		return client;
	}
}
