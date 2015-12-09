/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.messaging.kafka;


import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.state.ConsistentRegionContext;

@OutputPorts(@OutputPortSet(cardinality=1, optional=false, 
	description="Messages received from Kafka are sent on this output port."))
@PrimitiveOperator(name=KafkaSource.OPER_NAME, description=KafkaSource.DESC)
@Icons(location16="icons/KafkaConsumer_16.gif", location32="icons/KafkaConsumer_32.gif")
public class KafkaSource extends KafkaBaseOper {

	static final String OPER_NAME = "KafkaConsumer";
	private int threadsPerTopic = 1;
	private List<Integer> partitions = new ArrayList<Integer>();
	private static Logger trace = Logger.getLogger(KafkaSource.class.getName());
	
	KafkaConsumerClient streamsKafkaConsumer;
	private int consumerPollTimeout = 100;
	
	
	//consistent region checks
	@ContextCheck(compile = true)
	public static void checkInConsistentRegion(OperatorContextChecker checker) {
		ConsistentRegionContext consistentRegionContext = 
				checker.getOperatorContext().getOptionalContext(ConsistentRegionContext.class);

		if(consistentRegionContext != null ) {
			checker.setInvalidContext( OPER_NAME + " operator cannot be used inside a consistent region.", 
					new String[] {});
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
		ConsistentRegionContext crContext = getOperatorContext()
				.getOptionalContext(ConsistentRegionContext.class);
		if (crContext != null) {
			throw new IllegalArgumentException(
					"This operator does not support consistent region.");
		}
	}

	@Override
	public void allPortsReady() throws Exception {
		// initialize the client
		trace.log(TraceLevel.INFO, "Initializing source client");
		KafkaConsumerFactory clientFactory = new KafkaConsumerFactory();
		streamsKafkaConsumer = clientFactory.getClient(topicAH, keyAH, messageAH,
				partitions, consumerPollTimeout, finalProperties);
		streamsKafkaConsumer.init(getOutput(0), getOperatorContext()
				.getThreadFactory(), topics, threadsPerTopic);
	}

	@Parameter(name = "threadsPerTopic", optional = true, description = "Number of threads per topic. This parameter is only valid when using the HighLevelConsumer (specified zookeeper.connect instead of bootstrap.servers). Default is 1.")
	public void setThreadsPerTopic(int value) {
		this.threadsPerTopic = value;
	}

	@Parameter(name = "consumerPollTimeout", optional = true, description = "The time, in milliseconds, spent waiting in poll if data is not available. If 0, returns immediately with any records that are available now. Must not be negative. This parameter is only valid when using the KafkaConsumer(0.9) (specified bootstrap.servers instead of zookeeper.connect). Default is 100.")
	public void setConsumerPollTimeout(int value) {
		this.consumerPollTimeout = value;
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
			"This operator acts as a Kafka consumer receiving messages for a single topic. " +
			"Note that there may be multiple threads receiving messages depending on the configuration specified. " +
			"Ordering of messages is not guaranteed." + 
			"\\n\\n**Behavior in a Consistent Region**" + 
			"\\nThis operator cannot be used inside a consistent region."
			;
	
	@Override
	public void shutdown(){
		if (streamsKafkaConsumer != null){
			streamsKafkaConsumer.shutdown();
		}
	}

}

