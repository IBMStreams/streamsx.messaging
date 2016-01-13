package com.ibm.streamsx.messaging.kafka;

import java.util.Properties;
import java.util.logging.Logger;

import com.ibm.streams.operator.logging.TraceLevel;

public class KafkaProducerFactory {
	KafkaProducerClient client;
	private final Logger trace = Logger.getLogger(KafkaProducerFactory.class
			.getCanonicalName());
	
	public KafkaProducerClient getClient(AttributeHelper topicAH,
			AttributeHelper keyAH, AttributeHelper messageAH, Properties props) {
		if (messageAH.isString() && (!keyAH.isAvailable() || keyAH.isString())){
			trace.log(TraceLevel.WARNING, "Using KafkaProducer<String,String> client.");
			client = new ProducerStringHelper(topicAH, keyAH, messageAH, props);
		} else if ( !messageAH.isString() && (!keyAH.isAvailable() || !keyAH.isString())){
			trace.log(TraceLevel.WARNING, "Using KafkaProducer<byte,byte> client.");
			client = new ProducerByteHelper(topicAH, keyAH, messageAH, props);
		} else {
			trace.log(TraceLevel.ERROR, "Key and Message type must match and be of type String or Byte.");
		}

		return client;
	}
}
