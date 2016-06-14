package com.ibm.streamsx.messaging.kafka;

import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import com.ibm.streams.operator.logging.TraceLevel;

@SuppressWarnings("rawtypes")
public class KafkaConsumerFactory {
	KafkaConsumerClient client;
	private final Logger trace = Logger.getLogger(KafkaConsumerFactory.class
			.getCanonicalName());
	
	public KafkaConsumerClient getClient(AttributeHelper topicAH,
			AttributeHelper keyAH, AttributeHelper messageAH, List<Integer> partitions, int consumerPollTimeout, Properties props) throws UnsupportedStreamsKafkaConfigurationException {

		if (props.containsKey("bootstrap.servers")){
			props = KafkaConfigUtilities.setDefaultDeserializers(keyAH, messageAH, props);
			if (KafkaConfigUtilities.getStringProperty("value.deserializer",
					props).equalsIgnoreCase("org.apache.kafka.common.serialization.StringDeserializer")) {
				if (!messageAH.isString())
					trace.log(
							TraceLevel.WARN,
							"Using new 0.9 String consumer client even though output attribute is not of type String. This could hurt performance and we recommend using the same attribute type as your value.deserializer property.");
				trace.log(TraceLevel.INFO, "Using new 0.9 String consumer client.");
				client = new KafkaStringConsumerV9(topicAH, keyAH, messageAH,
						partitions, consumerPollTimeout, props);
			} else {
				if (!KafkaConfigUtilities
						.getStringProperty("value.deserializer", props)
						.equalsIgnoreCase("org.apache.kafka.common.serialization.ByteArrayDeserializer"))
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
				throw new UnsupportedOperationException("Missing required bootstrap.servers properties.");
			}
		}

		return client;
	}
}
