package com.ibm.streamsx.messaging.kafka;

import java.util.Properties;

public abstract class NewKafkaClient {
	AttributeHelper topicAH;
	AttributeHelper keyAH;
	AttributeHelper 	messageAH;
	Properties props;
	
	public NewKafkaClient(AttributeHelper topicAH, AttributeHelper keyAH,
			AttributeHelper messageAH, Properties props) {
		this.topicAH = topicAH;
		this.keyAH = keyAH;
		this.messageAH = messageAH;
		this.props = props;
	}
	
	abstract void shutdown();
	
}
