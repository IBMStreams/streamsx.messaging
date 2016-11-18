/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.messaging.kafka;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streams.operator.state.StateHandler;
import com.ibm.streamsx.messaging.common.DataGovernanceUtil;
import com.ibm.streamsx.messaging.common.IGovernanceConstants;

@InputPorts(@InputPortSet(cardinality=1, optional=false, 
	description="The tuples arriving on this port are expected to contain three attributes \\\"key\\\", \\\"topic\\\" and \\\"message\\\". " +
			"Out of these \\\"message\\\", is a required attribute."))
@PrimitiveOperator(name=KafkaSink.OPER_NAME, description=KafkaSink.DESC)
@Icons(location16="icons/KafkaProducer_16.gif", location32="icons/KafkaProducer_32.gif")
public class KafkaSink extends KafkaBaseOper implements StateHandler, Callback {
	
	
	static final String OPER_NAME =  "KafkaProducer";
	
	private static final Logger trace = Logger.getLogger(KafkaSink.class.getName());
	private KafkaProducerClient producerClient;
	
	// Sent messages that have not been sent to the broker by
	// the client.
	private final List<Future<RecordMetadata>> sentMessages =
	        Collections.synchronizedList(new LinkedList<>());
	private Exception sentMessageException; // synchronized by sentMessages
	private AtomicBoolean clientSeenException = new AtomicBoolean();
	
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
	
	private KafkaProducerClient getNewProducerClient(AttributeHelper topicAH, AttributeHelper keyAH, AttributeHelper messageAH, Properties finalProperties) {
		KafkaProducerFactory producerFactory = new KafkaProducerFactory();
		KafkaProducerClient producerClient = producerFactory.getClient(this, sentMessages, topicAH, keyAH, messageAH, finalProperties);
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
	public void process(StreamingInput<Tuple> stream, Tuple tuple) throws Exception {
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
		
		if (clientSeenException.get()){
			trace.log(TraceLevel.WARN, "Found message exception");
			resetProducerIfPropertiesHaveChanged();
		}

	}
	
	// If no more tuples are expected then make sure
	// the messages make it to the broker before
	// shutting down.
	@Override
	public void processPunctuation(StreamingInput<Tuple> stream, Punctuation mark) throws Exception {
	    if (mark == Punctuation.FINAL_MARKER)
	        drain();
	}
	
	
	private void resetProducerIfPropertiesHaveChanged()
			throws Exception {
		OperatorContext context = this.getOperatorContext();
		if (newPropertiesExist(context)){
			trace.log(TraceLevel.INFO,
					"Properties have changed. Initializing producer with new properties.");
			resetProducerClient(context);
		} else {
			trace.log(TraceLevel.INFO, "Properties have not changed, so we are keeping the same producer client and resetting the message exception.");
		}
		clientSeenException.set(false);
	}

	private void resetProducerClient(OperatorContext context) throws Exception {
	    
	    // Drain outstanding messages from the existing producer
	    drain();
		
		// Not catching exceptions because we want to fail
		// if we can't initialize a new producer
		getKafkaProperties(context);
		producerClient.shutdown();
		producerClient = getNewProducerClient(topicAH, keyAH, messageAH, finalProperties);
	}
	
	/**
	 * Delegate the removal of the task to a task to avoid any
	 * blocking or excessive processing on the Kafka I/O thread.
	 */
    @Override
    public void onCompletion(RecordMetadata rmd, Exception exception) {
        getOperatorContext().getScheduledExecutorService().submit(() -> handleCompletedMessage(rmd, exception));
    }

    /**
     * Handle a completed message. The callback cannot be tied
     * to a Future so just clean out any completed messages.
     */
    private Object handleCompletedMessage(RecordMetadata rmd, Exception exception) throws Exception {

        synchronized (sentMessages) {
            if (exception != null) {
                if (sentMessageException == null)
                    sentMessageException = exception;
            }

            final Iterator<Future<RecordMetadata>> it = sentMessages.iterator();
            while (it.hasNext()) {
                Future<RecordMetadata> sentMessage = it.next();
                try {
                    sentMessage.get(0, MILLISECONDS);
                } catch (TimeoutException e) {
                    // reached a non-completed message
                    // with the assumption messages are
                    // completed in order then stop
                    // looking for completed messages.
                    break;
                } catch (ExecutionException ee) {
                    trace.log(TraceLevel.ERROR, "Sending message failed", ee.getCause());

                    if (sentMessageException == null)
                        exception = ee;
                    // remove from list
                }
                it.remove();
            }
        }
        if (exception != null)
            throw exception;

        return null;
    }
	
	/**
     * Ensure any sent messages have been received by 
     * the broker according to the setting of the
     * Kafka ack configuration property.
     */
    @Override
    public void drain() throws Exception {

        outer: while (!sentMessages.isEmpty()) {

            synchronized (sentMessages) {
                
                if (sentMessageException != null)
                    throw sentMessageException;
                
                // Remove any completed messages, if there was an
                // exception then Future.get will throw an exception
                // causing the region to reset.
                final Iterator<Future<RecordMetadata>> it = sentMessages.iterator();
                while (it.hasNext()) {
                    Future<RecordMetadata> sentMessage = it.next();
                    try {
                        sentMessage.get(1, MILLISECONDS);
                    } catch (TimeoutException e) {
                        // Continue from the start of the
                        // list as it is expected messages
                        // will complete in order. And we don't
                        // wait to wait potentially for
                        // each unfinished message as the
                        // time could be up to N ms where
                        // N is the number of sent but
                        // not completed messages.
                        continue outer;
                    }
                    it.remove();
                }
            }
        }
    
        synchronized (sentMessages) {
            if (sentMessageException != null)
                throw sentMessageException;
        }
    }
    @Override
    public void checkpoint(Checkpoint checkpoint) {
        // nothing to save
    }
    @Override
    public void reset(Checkpoint checkpoint) {
        // Forget about any messages already sent.
        resetToInitialState();
    }
    @Override
    public void resetToInitialState() {
        // Forget about any messages already sent.
        synchronized (sentMessages) {
            sentMessageException = null;
            sentMessages.clear();
        }
        
    }
    @Override
    public void retireCheckpoint(long id) {
        // nothing to do.
    }
    @Override
    public void close() {
        // handled by shutdown     
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
			+ "The KafkaProducer guarantees at-least-once delivery of messages to a Kafka topic."
			;


}
