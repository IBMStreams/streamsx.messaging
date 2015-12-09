package com.ibm.streamsx.messaging.kafka;

import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import com.ibm.streams.operator.logging.TraceLevel;

public class KafkaConsumerFactory {
	KafkaConsumerClient client;
	private final Logger trace = Logger.getLogger(KafkaConsumerFactory.class
			.getCanonicalName());
	
	public KafkaConsumerClient getClient(AttributeHelper topicAH,
			AttributeHelper keyAH, AttributeHelper messageAH, List<Integer> partitions, int consumerPollTimeout, Properties props) throws UnsupportedStreamsKafkaConfigurationException {

		if (props.containsKey("bootstrap.servers")){
			if (KafkaConfigUtilities.getStringProperty("value.deserializer",
					props).equalsIgnoreCase(
					"org.apache.kafka.common.serialization.StringDeserializer")) {
				if (!messageAH.isString())
					trace.log(
							TraceLevel.WARN,
							"Using new 0.9 String consumer client even though output attribute is not of type String. This could hurt performance and we recommend using the same attribute type as your value.deserializer property.");
				trace.log(TraceLevel.INFO,
						"Using new 0.9 String consumer client.");
				client = new KafkaStringConsumerV9(topicAH, keyAH, messageAH,
						partitions, consumerPollTimeout, props);
			} else {
				if (!KafkaConfigUtilities
						.getStringProperty("value.deserializer", props)
						.equalsIgnoreCase(
								"org.apache.kafka.common.serialization.ByteArrayDeserializer"))
					throw new UnsupportedStreamsKafkaConfigurationException(
							"The specified deserializer is not supported by the KafkaSource.");
				
				if (messageAH.isString())
					trace.log(
							TraceLevel.WARN,
							"Using new 0.9 ByteArray consumer client even though output attribute is of type String. This could hurt performance and we recommend using the same attribute type as your value.deserializer property.");
				
				trace.log(TraceLevel.INFO,
						"Using new 0.9 ByteArray consumer client.");
				client = new KafkaByteArrayConsumerV9(topicAH, keyAH,
						messageAH, partitions, consumerPollTimeout, props);
			}
		} else {
			if(partitions != null && !partitions.isEmpty()){
				throw new UnsupportedOperationException("You are trying to use the high level consumer with a specified partions. Either remove partition arguments or make sure your bootstrap.servers property is set to use the Kafka 0.9 consumer.");
			}
				trace.log(TraceLevel.INFO, "Using High Level consumer client.");
				client = new KafkaHighLevelConsumer(topicAH, keyAH, messageAH, props);
		}

		return client;
	}
}
