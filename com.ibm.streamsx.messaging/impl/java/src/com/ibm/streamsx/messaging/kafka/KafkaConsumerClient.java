package com.ibm.streamsx.messaging.kafka;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.state.ConsistentRegionContext;

public abstract class KafkaConsumerClient extends KafkaBaseClient {
	Thread processThread;
	StreamingOutput<OutputTuple> streamingOutput;
	
	public KafkaConsumerClient(AttributeHelper topicAH, AttributeHelper keyAH,
			AttributeHelper messageAH, Properties props) {
		super(topicAH,keyAH,messageAH,props);
	}

	protected abstract void init(
			StreamingOutput<OutputTuple> so,
			ThreadFactory tf, List<String> topics);

	protected abstract Map<Integer, Long> getOffsetPositions() throws InterruptedException;

	protected abstract void seekToPositions(Map<Integer, Long> offsetMap);

	protected abstract void setConsistentRegionContext(ConsistentRegionContext crContext, int triggerCount);
	
}
