/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.messaging.kafka;


import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.ProcessingElement;
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
	private int a_partition = -1;
	private int triggerCount = -1;
	private int leaderConnectionRetries = 3;
	private int connectionRetryInterval = 1000;
	private static Logger trace = Logger.getLogger(KafkaSource.class.getName());
	KafkaConsumerClient newKafkaConsumer;
	
	
	//consistent region checks
	@ContextCheck(compile = true)
	public static void checkInConsistentRegion(OperatorContextChecker checker) {
		ConsistentRegionContext consistentRegionContext = 
				checker.getOperatorContext().getOptionalContext(ConsistentRegionContext.class);
		OperatorContext operContext = checker.getOperatorContext();

		if(consistentRegionContext != null ) {
			//checker.setInvalidContext( OPER_NAME + " operator cannot be used inside a consistent region.", 
			//		new String[] {});
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
				&& !operContext.getParameterNames().contains("kafkaProperty")){
			checker.setInvalidContext("Missing properties: Neither propertiesFile nor kafkaProperty parameters are set. At least one must be set.",new String[] {});

		}
		
	}
	
	//simple consumer client checks
	@ContextCheck(runtime = true, compile = false)
	public static void checkRuntimeCompatability(OperatorContextChecker checker) {
		OperatorContext operContext = checker.getOperatorContext();
		
		if (operContext.getParameterNames().contains("partition")){
			
			if (operContext.getParameterValues("topic").size() > 1){
				checker.setInvalidContext("Invalid topic parameter: Only one topic can be specified when the partition parameter is set.", new String[] {});
				throw new IllegalArgumentException("Invalid topic parameter: Only one topic can be specified when the partition parameter is set.");
			}
			
		}
		
	}
	
	@Override
	public void initialize(OperatorContext context)
			throws Exception {
		super.initialize(context);
		super.initSchema(getOutput(0).getStreamSchema());
		
		getOutput(0);
		if(threadsPerTopic < 1) 
			throw new IllegalArgumentException("Number of threads per topic cannot be less than one: " + threadsPerTopic);
		ConsistentRegionContext crContext = getOperatorContext().getOptionalContext(ConsistentRegionContext.class);
		if( crContext != null){
			
			if (a_partition == -1){
				throw new IllegalArgumentException("Partition parameter must be specified when using consistent region");
			}
			
		}
	}

	@Override
	public void allPortsReady() throws Exception {
		//initialize the client
		trace.log(TraceLevel.INFO, "Initializing client");

		if(a_partition >= 0){
			trace.log(TraceLevel.INFO, "Using simple consumer client.");
			simpleClient = new SimpleConsumerClient(topics.get(0),  a_partition, topicAH, keyAH, messageAH, finalProperties, triggerCount, leaderConnectionRetries, connectionRetryInterval);
			simpleClient.initialize(getOperatorContext());
			simpleClient.allPortsReady();
		} else {
			KafkaClientFactory clientFactory = new KafkaClientFactory();
			newKafkaConsumer = clientFactory.getClient(topicAH, keyAH, messageAH, finalProperties);
			newKafkaConsumer.init(getOutput(0), getOperatorContext().getThreadFactory(), topics, threadsPerTopic);
		}
	}

	@Parameter(name="threadsPerTopic", optional=true, 
			description="Number of threads per topic. Default is 1.")
	public void setThreadsPerTopic(int value) {
		this.threadsPerTopic = value;
	}	
	@Parameter(name="topic", cardinality=-1, optional=false, 
			description="Topic to be subscribed to.")
	public void setTopic(List<String> values) {
		if(values!=null)
			topics.addAll(values);
	}	
	
    @Parameter(name="partition", optional=true, 
			description="Partition to subscribe to. This must be set when using consistent region.")
	public void setPartition(int value) {
	   	this.a_partition = value;
	}
    
    @Parameter(name="triggerCount", optional=true, 
			description="Number of messages between checkpointing for consistent region. This is only relevant to operator driven checkpointing.")
	public void setTriggerCount(int value) {
	   	this.triggerCount = value;
	}
    
    @Parameter(name="leaderConnectionRetries", optional=true, 
			description="Number of attempts at finding a Kafka Broker before giving up. This is relevant when first looking for a broker, and in the case that a lead broker host goes down. This is only valid when the partition parameter is set. Default is 3.")
	public void setLeaderConnectionRetries(int value) {
	   	this.leaderConnectionRetries = value;
	}
    
    @Parameter(name="connectionRetryInterval", optional=true, 
			description="Interval between each attempt to find a lead Kafka Broker in milliseconds. Number of attempts is set by the leaderConnectionRetries parameter. This is only valid when the partition parameter is set. Default is 1000 ms.")
	public void setConnectionRetryInterval(int value) {
	   	this.connectionRetryInterval = value;
	}

	public static final String DESC = 
			"This operator acts as a Kafka consumer receiving messages for a single topic. " +
			"Note that there may be multiple threads receiving messages depending on the configuration specified. " +
			"Ordering of messages is not guaranteed." + 
			"\\n\\n**Behavior in a Consistent Region**" + 
			"\\nThis operator can be used inside a consistent region. Operator driven and periodical checkpointing " +
			"are supported. Partition to be read from must be specified. To consume multiple partitions in a topic, use " + 
			"user defined parallelism or multiple consumers."
			;

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void checkpoint(Checkpoint checkpoint) throws Exception {
		simpleClient.checkpoint(checkpoint);
	}

	@Override
	public void drain() throws Exception {
		simpleClient.drain();
		
	}

	@Override
	public void reset(Checkpoint checkpoint) throws Exception {
		simpleClient.reset(checkpoint);
	}

	@Override
	public void resetToInitialState() throws Exception {
		simpleClient.resetToInitialState();
	}

	@Override
	public void retireCheckpoint(long id) throws Exception {
		simpleClient.retireCheckpoint(id);
	}
	
	@Override
	public void shutdown(){
		if (newKafkaConsumer != null){
			newKafkaConsumer.shutdown();
		}
	}

}

