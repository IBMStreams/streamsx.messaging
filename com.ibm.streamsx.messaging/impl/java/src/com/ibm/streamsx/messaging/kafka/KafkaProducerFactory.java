/*******************************************************************************
 * Copyright (C) 2016, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.kafka;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.ibm.streams.operator.logging.TraceLevel;

public class KafkaProducerFactory {
	KafkaProducerClient client;
	private final Logger trace = Logger.getLogger(KafkaProducerFactory.class
			.getCanonicalName());
	
	public KafkaProducerClient getClient(
	        Callback callback, List<Future<RecordMetadata>> sentMessages,
	        AttributeHelper topicAH,
			AttributeHelper keyAH, AttributeHelper messageAH, Properties props) {
		if (messageAH.isString() && (!keyAH.isAvailable() || keyAH.isString())){
			trace.log(TraceLevel.WARNING, "Using KafkaProducer<String,String> client.");
			client = new ProducerStringHelper(callback, sentMessages, topicAH, keyAH, messageAH, props);
		} else if ( !messageAH.isString() && (!keyAH.isAvailable() || !keyAH.isString())){
			trace.log(TraceLevel.WARNING, "Using KafkaProducer<byte,byte> client.");
			client = new ProducerByteHelper(callback, sentMessages, topicAH, keyAH, messageAH, props);
		} else {
			trace.log(TraceLevel.ERROR, "Key and Message type must match and be of type String or Byte.");
		}

		return client;
	}
}
