/*******************************************************************************
 * Copyright (C) 2016, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.kafka;

import java.util.Properties;
import java.util.logging.Logger;

public abstract class KafkaBaseClient {
	AttributeHelper topicAH;
	AttributeHelper keyAH;
	AttributeHelper 	messageAH;
	Properties props;
	
	static final Logger trace = Logger.getLogger(KafkaBaseClient.class.getCanonicalName());
	
	public KafkaBaseClient(AttributeHelper topicAH, AttributeHelper keyAH,
			AttributeHelper messageAH, Properties props) {
		this.topicAH = topicAH;
		this.keyAH = keyAH;
		this.messageAH = messageAH;
		this.props = props;
	}
	
	abstract void shutdown();
	
}
