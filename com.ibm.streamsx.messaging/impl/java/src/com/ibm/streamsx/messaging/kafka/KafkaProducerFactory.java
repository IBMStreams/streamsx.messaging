/*******************************************************************************
 * Copyright (C) 2016, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
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
			trace.log(TraceLevel.WARNING, "Using KafkaProducer<String,String> client."); //$NON-NLS-1$
			client = new ProducerStringHelper(topicAH, keyAH, messageAH, props);
		} else if ( !messageAH.isString() && (!keyAH.isAvailable() || !keyAH.isString())){
			trace.log(TraceLevel.WARNING, "Using KafkaProducer<byte,byte> client."); //$NON-NLS-1$
			client = new ProducerByteHelper(topicAH, keyAH, messageAH, props);
		} else {
			trace.log(TraceLevel.ERROR, Messages.getString("KEY_AND_MESSAGE_TYPE_MUST_MATCH_AND_BE_STRING_OR_BYTE")); //$NON-NLS-1$
		}

		return client;
	}
}
