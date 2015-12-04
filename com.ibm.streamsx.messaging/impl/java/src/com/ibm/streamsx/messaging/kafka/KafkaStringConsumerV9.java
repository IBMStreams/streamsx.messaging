package com.ibm.streamsx.messaging.kafka;

import java.util.Properties;

import com.ibm.streams.operator.OutputTuple;

public class KafkaStringConsumerV9 extends KafkaConsumerV9<String,String>{
	
	public KafkaStringConsumerV9(AttributeHelper topicAH,
			AttributeHelper keyAH, AttributeHelper messageAH, Properties props) {
		super(topicAH, keyAH, messageAH, props);
	}

	@Override
	protected void setMessageValue(AttributeHelper messageAH, OutputTuple otup,
			String message) {
		messageAH.setValue(otup, message);
	}

	@Override
	protected void setKeyValue(AttributeHelper keyAH, OutputTuple otup, String key) {
		keyAH.setValue(otup, key);
	}

}
