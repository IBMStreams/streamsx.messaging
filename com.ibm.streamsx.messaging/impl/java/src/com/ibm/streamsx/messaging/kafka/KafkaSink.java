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
	
	
	static final String OPER_NAME =  "KafkaProducer"; //$NON-NLS-1$
	
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
		return checker.checkExcludedParameters("topic", "topicAttribute") && //$NON-NLS-1$ //$NON-NLS-2$
			   checker.checkExcludedParameters("topicAttribute", "topic"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	//consistent region checks
	@ContextCheck(compile = true)
	public static void checkInConsistentRegion(OperatorContextChecker checker) {
		ConsistentRegionContext consistentRegionContext = 
				checker.getOperatorContext().getOptionalContext(ConsistentRegionContext.class);
		
		if(consistentRegionContext != null && consistentRegionContext.isStartOfRegion()) {
			checker.setInvalidContext( Messages.getString("CANNOT_BE_START_OF_CONSISTENT_REGION"),  //$NON-NLS-1$
					new String[] {OPER_NAME});
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
			throw new IllegalArgumentException(Messages.getString("TOPIC_NOT_SPECIFIED")); //$NON-NLS-1$
		
		if(keyAH.isAvailable() && ( keyAH.isString() != messageAH.isString())) {
			throw new IllegalArgumentException(Messages.getString("KEY_AND_MESSAGE_MUST_HAVE_COMPATIBLE_TYPES")); //$NON-NLS-1$
		}
		
		if(!topics.isEmpty())
			trace.log(TraceLevel.INFO, "Topics: " + topics.toString()); //$NON-NLS-1$

		trace.log(TraceLevel.INFO, "Initializing producer"); //$NON-NLS-1$
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
		trace.log(TraceLevel.INFO, "KafkaSink -- Registering for data governance"); //$NON-NLS-1$

		if (!topics.isEmpty()) {
			for (String topic : topics) {
				trace.log(TraceLevel.INFO, OPER_NAME + " -- data governance - topic to register: " + topic); //$NON-NLS-1$
				DataGovernanceUtil.registerForDataGovernance(this, topic, IGovernanceConstants.ASSET_KAFKA_TOPIC_TYPE,
						null, null, false, "KafkaSink"); //$NON-NLS-1$
			}
		} else {
			trace.log(TraceLevel.INFO, "KafkaSink -- Registering for data governance -- topics is empty"); //$NON-NLS-1$
		}

	}
	
	@Override
	public void process(StreamingInput<Tuple> stream, Tuple tuple) throws FileNotFoundException, IOException, UnsupportedStreamsKafkaConfigurationException{
		try {	
			if(trace.isLoggable(TraceLevel.DEBUG))
				trace.log(TraceLevel.DEBUG, "Sending message: " + tuple); //$NON-NLS-1$
			
			if(!topics.isEmpty()) 
				producerClient.send(tuple, topics);
			else 
				producerClient.send(tuple);
		} catch(Exception e) {
			trace.log(TraceLevel.ERROR, "Could not send message: " + tuple, e); //$NON-NLS-1$
			e.printStackTrace();
			resetProducerIfPropertiesHaveChanged();
		}
		
		if(producerClient.hasMessageException() == true){
			trace.log(TraceLevel.WARN, "Found message exception"); //$NON-NLS-1$
			resetProducerIfPropertiesHaveChanged();
		}

	}
	private void resetProducerIfPropertiesHaveChanged()
			throws FileNotFoundException, IOException, UnsupportedStreamsKafkaConfigurationException {
		OperatorContext context = this.getOperatorContext();
		if (newPropertiesExist(context)){
			trace.log(TraceLevel.INFO,
					"Properties have changed. Initializing producer with new properties."); //$NON-NLS-1$
			resetProducerClient(context);
		} else {
			trace.log(TraceLevel.INFO, "Properties have not changed, so we are keeping the same producer client and resetting the message exception."); //$NON-NLS-1$
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
			"This operator acts as a Kafka producer sending tuples as messages to a Kafka broker. " +  //$NON-NLS-1$
			"The broker is assumed to be already configured and running. " + //$NON-NLS-1$
			"The incoming stream can have three attributes: topic, key and message. " + //$NON-NLS-1$
			"The message is a required attribute. " + //$NON-NLS-1$
			"A topic can be specified as either an input stream attribute or as a parameter. " + //$NON-NLS-1$
			BASE_DESC + // common description between Source and Sink
			"\\n\\n**Behavior in a Consistent Region**" +  //$NON-NLS-1$
			"\\nThis operator can participate in a consistent region.  This operator cannot be placed at the start of a consistent region. " //$NON-NLS-1$
			+ "The KafkaProducer guarantees at-least-once delivery of messages to a Kafka topic." //$NON-NLS-1$
			;
}
