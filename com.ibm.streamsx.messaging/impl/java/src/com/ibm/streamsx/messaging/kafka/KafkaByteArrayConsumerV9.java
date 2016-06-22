/*******************************************************************************
 * Copyright (C) 2016, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.kafka;

import java.util.List;
import java.util.Properties;

import com.ibm.streams.operator.OutputTuple;

public class KafkaByteArrayConsumerV9 extends KafkaConsumerClient<byte[],byte[]>{
	
	public KafkaByteArrayConsumerV9(AttributeHelper topicAH,
			AttributeHelper keyAH, AttributeHelper messageAH, List<Integer> partitions, int consumerPollTimeout, Properties props) {
		super(topicAH, keyAH, messageAH, partitions, props);
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
