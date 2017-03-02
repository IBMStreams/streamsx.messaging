/*******************************************************************************
 * Copyright (C) 2016, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
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

		if (props.containsKey("bootstrap.servers")){ //$NON-NLS-1$
			props = KafkaConfigUtilities.setDefaultDeserializers(keyAH, messageAH, props);
			if (KafkaConfigUtilities.getStringProperty("value.deserializer", //$NON-NLS-1$
					props).equalsIgnoreCase("org.apache.kafka.common.serialization.StringDeserializer")) { //$NON-NLS-1$
				if (!messageAH.isString())
					trace.log(
							TraceLevel.WARN,
							"Using new 0.9 String consumer client even though output attribute is not of type String. This could hurt performance and we recommend using the same attribute type as your value.deserializer property."); //$NON-NLS-1$
				trace.log(TraceLevel.INFO, "Using new 0.9 String consumer client."); //$NON-NLS-1$
				client = new KafkaStringConsumerV9(topicAH, keyAH, messageAH,
						partitions, consumerPollTimeout, props);
			} else {
				if (!KafkaConfigUtilities
						.getStringProperty("value.deserializer", props) //$NON-NLS-1$
						.equalsIgnoreCase("org.apache.kafka.common.serialization.ByteArrayDeserializer")) //$NON-NLS-1$
					throw new UnsupportedStreamsKafkaConfigurationException(
							Messages.getString("SPECIFIED_DESERIALIZER_NOT_SUPPORTED")); //$NON-NLS-1$
				
				if (messageAH.isString())
					trace.log(
							TraceLevel.WARN,
							"Using new 0.9 ByteArray consumer client even though output attribute is of type String. This could hurt performance and we recommend using the same attribute type as your value.deserializer property."); //$NON-NLS-1$
				
				trace.log(TraceLevel.INFO,
						"Using new 0.9 ByteArray consumer client."); //$NON-NLS-1$
				client = new KafkaByteArrayConsumerV9(topicAH, keyAH,
						messageAH, partitions, consumerPollTimeout, props);
			}
		} else {
			if(partitions != null && !partitions.isEmpty()){
				throw new UnsupportedOperationException(Messages.getString("MISSING_REQUIRED_BOOTSTRAP_SERVERS_PROPS")); //$NON-NLS-1$
			}
		}

		return client;
	}
}
