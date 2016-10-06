/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.messaging.kafka;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streamsx.messaging.common.DataGovernanceUtil;
import com.ibm.streamsx.messaging.common.IGovernanceConstants;

@InputPorts(@InputPortSet(cardinality=1, optional=false, 
	description="The tuples arriving on this port are expected to contain three attributes \\\"key\\\", \\\"topic\\\" and \\\"message\\\". " +
			"Out of these \\\"message\\\", is a required attribute."))
@PrimitiveOperator(name=KafkaSink.OPER_NAME, description=KafkaSink.DESC)
@Icons(location16="icons/KafkaProducer_16.gif", location32="icons/KafkaProducer_32.gif")
public class KafkaSink extends KafkaBaseOper {
	
	
	static final String OPER_NAME =  "KafkaProducer";
	
	private static final Logger trace = Logger.getLogger(KafkaSink.class.getName());
	private KafkaProducerClient producerClient;
	
	@Parameter(name="topic", cardinality=-1, optional=true, 
			description="Topic to be published to. A topic can also be specified as an input stream attribute.")
	public void setTopic(List<String> values) {
		if(values!=null)
			topics.addAll(values);
	}	
	@Parameter(optional=true, description="Name of the attribute for the topic. Default is \\\"topic\\\"")
	public void setTopicAttribute(String value) {
		topicAH.setName(value);
	}
	
	@ContextCheck(compile=true)
	public static boolean topicChecker(OperatorContextChecker checker) {
		return checker.checkExcludedParameters("topic", "topicAttribute") &&
			   checker.checkExcludedParameters("topicAttribute", "topic");
	}

	//consistent region checks
	@ContextCheck(compile = true)
	public static void checkInConsistentRegion(OperatorContextChecker checker) {
		ConsistentRegionContext consistentRegionContext = 
				checker.getOperatorContext().getOptionalContext(ConsistentRegionContext.class);
		
		if(consistentRegionContext != null && consistentRegionContext.isStartOfRegion()) {
			checker.setInvalidContext( OPER_NAME + " operator cannot be placed at the start of consistent region.", 
					new String[] {});
		}
	}
	
	//check for message attribute
	@ContextCheck(runtime = true, compile=false)
	public static void checkIncomingMessageAttribute(OperatorContextChecker checker) throws Exception {
		OperatorContext operContext = checker.getOperatorContext();
		StreamSchema operSchema = operContext.getStreamingInputs().get(0).getStreamSchema();
		checkForMessageAttribute(operContext, operSchema);		
	}

	@Override
	public void initialize(OperatorContext context) throws Exception
			{
		super.initialize(context);
		super.initSchema(getInput(0).getStreamSchema());
		
		if(topics.size() == 0 && !topicAH.isAvailable())
			throw new IllegalArgumentException("Topic has not been specified. Specify either the \"topicAttribute\" or \"topic\" parameters.");
		
		if(keyAH.isAvailable() && ( keyAH.isString() != messageAH.isString())) {
			throw new IllegalArgumentException("Key and Message attributes must have compatible types.");
		}
		
		if(!topics.isEmpty())
			trace.log(TraceLevel.INFO, "Topics: " + topics.toString());

		trace.log(TraceLevel.INFO, "Initializing producer");
		producerClient = getNewProducerClient(topicAH, keyAH, messageAH, finalProperties);
		
		// register for data governance
		// only register user specified topic in param
		registerForDataGovernance();
	}
	
	private static KafkaProducerClient getNewProducerClient(AttributeHelper topicAH, AttributeHelper keyAH, AttributeHelper messageAH, Properties finalProperties) {
		KafkaProducerFactory producerFactory = new KafkaProducerFactory();
		KafkaProducerClient producerClient = producerFactory.getClient(topicAH, keyAH, messageAH, finalProperties);
		return producerClient;
	}

	private void registerForDataGovernance() {
		trace.log(TraceLevel.INFO, "KafkaSink -- Registering for data governance");

		if (!topics.isEmpty()) {
			for (String topic : topics) {
				trace.log(TraceLevel.INFO, OPER_NAME + " -- data governance - topic to register: " + topic);
				DataGovernanceUtil.registerForDataGovernance(this, topic, IGovernanceConstants.ASSET_KAFKA_TOPIC_TYPE,
						null, null, false, "KafkaSink");
			}
		} else {
			trace.log(TraceLevel.INFO, "KafkaSink -- Registering for data governance -- topics is empty");
		}

	}
	
	@Override
	public void process(StreamingInput<Tuple> stream, Tuple tuple) throws FileNotFoundException, IOException, UnsupportedStreamsKafkaConfigurationException{
		try {	
			if(trace.isLoggable(TraceLevel.DEBUG))
				trace.log(TraceLevel.DEBUG, "Sending message: " + tuple);
			
			if(!topics.isEmpty()) 
				producerClient.send(tuple, topics);
			else 
				producerClient.send(tuple);
		} catch(Exception e) {
			trace.log(TraceLevel.ERROR, "Could not send message: " + tuple, e);
			e.printStackTrace();
			resetProducerIfPropertiesHaveChanged();
		}
		
		if(producerClient.hasMessageException() == true){
			trace.log(TraceLevel.WARN, "Found message exception");
			resetProducerIfPropertiesHaveChanged();
		}

	}
	private void resetProducerIfPropertiesHaveChanged()
			throws FileNotFoundException, IOException, UnsupportedStreamsKafkaConfigurationException {
		OperatorContext context = this.getOperatorContext();
		if (newPropertiesExist(context)){
			trace.log(TraceLevel.INFO,
					"Properties have changed. Initializing producer with new properties.");
			resetProducerClient(context);
		} else {
			trace.log(TraceLevel.INFO, "Properties have not changed, so we are keeping the same producer client and resetting the message exception.");
			producerClient.resetMessageException();
		}
	}

	private void resetProducerClient(OperatorContext context) throws FileNotFoundException, IOException, UnsupportedStreamsKafkaConfigurationException {
		
		// Not catching exceptions because we want to fail
		// if we can't initialize a new producer
		getKafkaProperties(context);
		producerClient.shutdown();
		producerClient = getNewProducerClient(topicAH, keyAH, messageAH, finalProperties);
	}

	public static final String DESC = 
			"This operator acts as a Kafka producer sending tuples as messages to a Kafka broker. " + 
			"The broker is assumed to be already configured and running. " +
			"The incoming stream can have three attributes: topic, key and message. " +
			"The message is a required attribute. " +
			"A topic can be specified as either an input stream attribute or as a parameter. " +
			BASE_DESC + // common description between Source and Sink
			"\\n\\n**Behavior in a Consistent Region**" + 
			"\\nThis operator can participate in a consistent region.  This operator cannot be placed at the start of a consistent region. "
			;
}
