/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.messaging.kafka;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streams.operator.state.StateHandler;

@OutputPorts(@OutputPortSet(cardinality=1, optional=false, 
	description="Messages received from Kafka are sent on this output port."))
@PrimitiveOperator(name=KafkaSource.OPER_NAME, description=KafkaSource.DESC)
@Icons(location16="icons/KafkaConsumer_16.gif", location32="icons/KafkaConsumer_32.gif")
public class KafkaSource extends KafkaBaseOper implements StateHandler{

	static final String OPER_NAME = "KafkaConsumer";
	private int threadsPerTopic = 1;
	private List<Integer> partitions = new ArrayList<Integer>();
	private static Logger trace = Logger.getLogger(KafkaSource.class.getName());
	
	KafkaConsumerClient streamsKafkaConsumer;
	private int consumerPollTimeout = 100;
	private int triggerCount = -1;
	
	//consistent region checks
	@ContextCheck(compile = true)
	public static void checkInConsistentRegion(OperatorContextChecker checker) {
		ConsistentRegionContext consistentRegionContext = 
				checker.getOperatorContext().getOptionalContext(ConsistentRegionContext.class);
		OperatorContext operContext = checker.getOperatorContext();

		if(consistentRegionContext != null ) {
			if (!operContext.getParameterNames().contains("partition")){
				checker.setInvalidContext("The partition parameter must be specified in consistent regions.", new String[] {});
			}
		}
	}
	
	//simple consumer client checks
	@ContextCheck(runtime = false, compile = true)
	public static void checkCompileCompatability(OperatorContextChecker checker) {
		OperatorContext operContext = checker.getOperatorContext();

		if (!operContext.getParameterNames().contains("propertiesFile")
				&& !operContext.getParameterNames().contains("kafkaProperty")) {
			checker.setInvalidContext(
					"Missing properties: Neither propertiesFile nor kafkaProperty parameters are set. At least one must be set.",
					new String[] {});

		}

	}

	@ContextCheck(runtime = true, compile = false)
	public static void checkRuntimeCompatability(OperatorContextChecker checker) {
		OperatorContext operContext = checker.getOperatorContext();

		if (operContext.getParameterNames().contains("partition")) {

			if (operContext.getParameterValues("topic").size() > 1) {
				checker.setInvalidContext(
						"Invalid topic parameter: Only one topic can be specified when the partition parameter is set.",
						new String[] {});
				throw new IllegalArgumentException(
						"Invalid topic parameter: Only one topic can be specified when the partition parameter is set.");
			}

		}

	}
	
	//check for message attribute
	@ContextCheck(runtime = true, compile=false)
	public static void checkIncomingMessageAttribute(OperatorContextChecker checker) throws Exception {
		OperatorContext operContext = checker.getOperatorContext();
		StreamSchema operSchema = operContext.getStreamingOutputs().get(0).getStreamSchema();
		checkForMessageAttribute(operContext, operSchema);		
	}
	
	@Override
	public void initialize(OperatorContext context) throws Exception {
		super.initialize(context);
		super.initSchema(getOutput(0).getStreamSchema());

		getOutput(0);
		if (threadsPerTopic < 1)
			throw new IllegalArgumentException(
					"Number of threads per topic cannot be less than one: "
							+ threadsPerTopic);

		// initialize the client
		trace.log(TraceLevel.INFO, "Initializing source client");
		KafkaConsumerFactory clientFactory = new KafkaConsumerFactory();
		streamsKafkaConsumer = clientFactory.getClient(topicAH, keyAH, messageAH,
				partitions, consumerPollTimeout, finalProperties);
		consistentRegionCheckAndSetup();

	}

	@Override
	public void allPortsReady() throws Exception {
		streamsKafkaConsumer.init(getOutput(0), getOperatorContext()
				.getThreadFactory(), topics);
	}

	
	
	private void consistentRegionCheckAndSetup() {
		ConsistentRegionContext crContext = getOperatorContext()
				.getOptionalContext(ConsistentRegionContext.class);
		if (crContext != null){
			streamsKafkaConsumer.setConsistentRegionContext(crContext, triggerCount);
		}
	}

	@Parameter(name = "consumerPollTimeout", optional = true, description = "The time, in milliseconds, spent waiting in poll if data is not available. If 0, returns immediately with any records that are available now. Must not be negative. This parameter is only valid when using the KafkaConsumer(0.9) (specified bootstrap.servers instead of zookeeper.connect). Default is 100.")
	public void setConsumerPollTimeout(int value) {
		this.consumerPollTimeout = value;
	}
	
    @Parameter(name="triggerCount", optional=true, 
			description="Approximate number of messages between checkpointing for consistent region. This is only relevant to operator driven checkpointing. Checkpointing is done after a buffer of messages is submitted, so actual triggerCount at checkpoint time may be slightly above specified triggerCount.")
	public void setTriggerCount(int value) {
	   	this.triggerCount = value;
	}

	@Parameter(name = "topic", cardinality = -1, optional = false, description = "Topic to be subscribed to. 1 or more can be provided using comma separation. Ex: \\\"mytopic1\\\",\\\"mytopic2\\\"")
	public void setTopic(List<String> values) {
		if (values != null)
			topics.addAll(values);
	}

	@Parameter(name = "partition", cardinality = -1, optional = true, description = "Partition to be subscribed to. 1 or more can be provided using comma separation. You may only specify 1 topic if you are specifying partitions. Ex: 0,2,3")
	public void setPartition(int[] values) {
		for (int index = 0; index < values.length; index++) {
			partitions.add(values[index]);
		}
	}

	public static final String DESC = 
			"This operator acts as a Kafka consumer receiving messages for one or more topics. " +
			"For parallel consumption, we strongly recommend specifying partitions on each Consumer operator, " +
			"as we have found the automatic partition assignment strategies from Kafka to be unreliable." + 
			"Ordering of messages is only guaranteed per Kafka topic partition." + 
			"\\n\\n**Behavior in a Consistent Region**" + 
			"\\nThis operator can be used inside a consistent region. Operator driven and periodical checkpointing " +
			"are supported. Partitions to be read from must be specified. " +
			"Resetting to initial state is not supported because the intial offset cannot be saved and may not be present in the Kafka log. " + 
			"In the case of a reset to initial state after operator crash, messages will start being read from the time of reset."
			;
	
	@Override
	public void shutdown() throws Exception{
		if (streamsKafkaConsumer != null){
			streamsKafkaConsumer.shutdown();
		}
		super.shutdown();
	}

	@Override
	public void checkpoint(Checkpoint checkpoint) throws Exception {
		Map<Integer, Long> offsetMap = streamsKafkaConsumer.getOffsetPositions();
		trace.log(TraceLevel.INFO, "Checkpointing offsetMap.");
		checkpoint.getOutputStream().writeObject(offsetMap);
	}

	@Override
	public void drain() throws Exception {
		trace.log(TraceLevel.INFO,"Draining....");
	}

	@Override
	public void reset(Checkpoint checkpoint) throws Exception {
		@SuppressWarnings("unchecked")
		Map<Integer, Long> offsetMap = (Map<Integer, Long>) checkpoint.getInputStream().readObject();
		trace.log(TraceLevel.INFO, "Resetting...");
		streamsKafkaConsumer.seekToPositions(offsetMap);		
	}

	@Override
	public void resetToInitialState() throws Exception {
		trace.log(TraceLevel.INFO, "Resetting to initial state. Consumer will begin consuming from the latest offset.");
	}

	@Override
	public void retireCheckpoint(long id) throws Exception {
		trace.log(TraceLevel.INFO, "Retiring Checkpoint.");
	}

	@Override
	public void close() throws IOException {
		
	}

}

