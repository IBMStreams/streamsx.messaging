package com.ibm.streamsx.messaging.kafka;

import java.util.Properties;

import com.ibm.streams.operator.OutputTuple;

public class KafkaByteArrayConsumerV9 extends KafkaConsumerV9<byte[],byte[]>{
	
	public KafkaByteArrayConsumerV9(AttributeHelper topicAH,
			AttributeHelper keyAH, AttributeHelper messageAH, Properties props) {
		super(topicAH, keyAH, messageAH, props);
	}

	@Override
	protected void setMessageValue(AttributeHelper messageAH, OutputTuple otup,
			byte[] message) {
		messageAH.setValue(otup, message);
	}

	@Override
	protected void setKeyValue(AttributeHelper keyAH, OutputTuple otup, byte[] key) {
		keyAH.setValue(otup, key);
	}

}
