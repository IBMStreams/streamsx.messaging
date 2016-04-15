/*******************************************************************************
 * Copyright (C) 2013, 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.jms;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.naming.NamingException;
import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.logging.LogLevel;
import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.model.CustomMetric;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.samples.patterns.ProcessTupleProducer;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streams.operator.state.StateHandler;
import com.ibm.streams.operator.types.RString;
import com.ibm.streamsx.messaging.common.DataGovernanceUtil;
import com.ibm.streamsx.messaging.common.IGovernanceConstants;

//The JMSSource operator converts a message JMS queue or topic to stream
public class JMSSource extends ProcessTupleProducer implements StateHandler{	

	private static final String CLASS_NAME = "com.ibm.streamsx.messaging.jms.JMSSource";

	/**
	 * Create a {@code Logger} specific to this class that will write to the SPL
	 * log facility as a child of the {@link LoggerNames#LOG_FACILITY}
	 * {@code Logger}. The {@code Logger} uses a
	 */
	private static Logger logger = Logger.getLogger(LoggerNames.LOG_FACILITY
			+ "." + CLASS_NAME, "com.ibm.streamsx.messaging.jms.JMSMessages");

	// variable to hold the output port
	private StreamingOutput<OutputTuple> dataOutputPort;

	// Variables required by the optional error output port

	// hasErrorPort signifies if the operator has error port defined or not
	// assuming in the beginning that the operator does not have a error output
	// port by setting hasErrorPort to false
	// further down in the code, if the no of output ports is 2, we set to true
	// We send data to error ouput port only in case where hasErrorPort is set
	// to true which implies that the opeator instance has a error output port
	// defined.
	private boolean hasErrorPort = false;
	// Variable to specify error output port
	private StreamingOutput<OutputTuple> errorOutputPort;

	// Create an instance of class JMSConnectionhelper which is responsible for
	// creating and maintaining connection and receiving messages from JMS
	// Provider
	JMSConnectionHelper jmsConnectionHelper;

	// Variable to hold the message type as received in the connections
	// document.
	private MessageClass messageType;
	// Variable to specify if the JSMProvider is Apache Active MQ or WebSphere
	// MQ
	// set to true for Apache Active MQ.
	private boolean isAMQ;
	
	// consistent region context
    private ConsistentRegionContext consistentRegionContext;

	// Variables to hold performance metrices for JMSSource

	// nMessagesRead is the number of messages read successfully.
	// nMessagesDropped is the number of messages dropped.
	// nReconnectionAttempts is the number of reconnection attempts made before
	// a successful connection.

	Metric nMessagesRead;
	Metric nMessagesDropped;
	Metric nReconnectionAttempts;
	
	// when in consistent region, this parameter is used to indicate max time the receive method should block
	public static final long RECEIVE_TIMEOUT = 500l;

	// initialize the metrices.
	@CustomMetric(kind = Metric.Kind.COUNTER)
	public void setnMessagesRead(Metric nMessagesRead) {
		this.nMessagesRead = nMessagesRead;
	}

	@CustomMetric(kind = Metric.Kind.COUNTER)
	public void setnMessagesDropped(Metric nMessagesDropped) {
		this.nMessagesDropped = nMessagesDropped;
	}

	@CustomMetric(kind = Metric.Kind.COUNTER)
	public void setnReconnectionAttempts(Metric nReconnectionAttempts) {
		this.nReconnectionAttempts = nReconnectionAttempts;
	}

	// operator parameters

	// This optional parameter codepage speciifes the code page of the target
	// system using which ustring conversions have to be done for a BytesMessage
	// type.
	// If present, it must have exactly one value that is a String constant. If
	// the parameter is absent, the operator will use the default value of to
	// UTF-8
	private String codepage = "UTF-8";
	// This mandatory parameter access specifies access specification name.
	private String access;
	// This mandatory parameter connection specifies name of the connection
	// specification containing a JMS element
	private String connection;
	// This optional parameter connectionDocument specifies the pathname of a
	// file containing the connection information.
	// If present, it must have exactly one value that is a String constant.
	// If the parameter is absent, the operator will use the default location
	// filepath etc/connections.xml (with respect to the application directory)
	private String connectionDocument = null;
	// This optional parameter reconnectionBound specifies the number of
	// successive connections that
	// will be attempted for this operator.
	// It is an optional parameter of type uint32.
	// It can appear only when the reconnectionPolicy parameter is set to
	// BoundedRetry and cannot appear otherwise.If not present the default value
	// is taken to be 5.
	private int reconnectionBound = 5;
	// This optional parameter reconnectionPolicy specifies the reconnection
	// policy that would be applicable during initial/intermittent connection
	// failures.
	// The valid values for this parameter are NoRetry, BoundedRetry and
	// InfiniteRetry.
	// If not specified, it is set to BoundedRetry with a reconnectionBound of 5
	// and a period of 60 seconds.
	private ReconnectionPolicies reconnectionPolicy = ReconnectionPolicies
			.valueOf("BoundedRetry");
	// This optional parameter period specifies the time period in seconds which
	// the
	// operator will wait before trying to reconnect.
	// It is an optional parameter of type float64.
	// If not specified, the default value is 60.0. It must appear only when the
	// reconnectionPolicy parameter is specified
	private double period = 60.0;
	
	// Declaring the JMSMEssagehandler,
	private JMSMessageHandlerImpl messageHandlerImpl;
	
	// Specify after how many messages are received, the operator should establish consistent region
	private int triggerCount = 0;

	// instance of JMSSourceCRState to hold variables required for consistent region
	private JMSSourceCRState crState = null;
	
	private String messageSelector = null;
	
	private boolean initalConnectionEstablished = false;
	
	private String messageIDOutAttrName = null;
	
	private Object resetLock = new Object();

	public String getMessageIDOutAttrName() {
		return messageIDOutAttrName;
	}

	@Parameter(optional = true)
	public void setMessageIDOutAttrName(String messageIDOutAttrName) {
		this.messageIDOutAttrName = messageIDOutAttrName;
	}

	public String getMessageSelector() {
		return messageSelector;
	}

	@Parameter(optional = true)
	public void setMessageSelector(String messageSelector) {
		this.messageSelector = messageSelector;
	}

	public int getTriggerCount() {
		return triggerCount;
	}

	@Parameter(optional = true)
	public void setTriggerCount(int triggerCount) {
		this.triggerCount = triggerCount;
	}

	// Mandatory parameter access
	@Parameter(optional = false)
	public void setAccess(String access) {
		this.access = access;
	}

	// Mandatory parameter connection
	@Parameter(optional = false)
	public void setConnection(String connection) {
		this.connection = connection;
	}

	// Optional parameter codepage
	@Parameter(optional = true)
	public void setCodepage(String codepage) {
		this.codepage = codepage;
	}

	// Optional parameter reconnectionPolicy
	@Parameter(optional = true)
	public void setReconnectionPolicy(String reconnectionPolicy) {
		this.reconnectionPolicy = ReconnectionPolicies
				.valueOf(reconnectionPolicy);
	}

	// Optional parameter reconnectionBound
	@Parameter(optional = true)
	public void setReconnectionBound(int reconnectionBound) {
		this.reconnectionBound = reconnectionBound;
	}

	// Optional parameter period
	@Parameter(optional = true)
	public void setPeriod(double period) {
		this.period = period;
	}

	// Optional parameter connectionDocument
	@Parameter(optional = true, description="Connection document containing connection information to connect to messaging servers.  If not specified, the connection document is assumed to be application_dir/etc/connections.xml.")
	public void setConnectionDocument(String connectionDocument) {
		this.connectionDocument = connectionDocument;
	}
	
	// Class to hold various variables for consistent region
	private class JMSSourceCRState {
		
		// Last fully processed message
		private Message lastMsgSent;
		
		// Counters for counting number of received messages
		private int msgCounter;
		
		// Flag to indicate a checkpoint has been made.
		private boolean isCheckpointPerformed;
		
		private List<String> msgIDWIthSameTS;
		
		JMSSourceCRState() {
			lastMsgSent = null;
			msgCounter = 0;
			isCheckpointPerformed = false;
			msgIDWIthSameTS = new ArrayList<String>();
		}

		public List<String> getMsgIDWIthSameTS() {
			return msgIDWIthSameTS;
		}

		public boolean isCheckpointPerformed() {
			return isCheckpointPerformed;
		}

		public void setCheckpointPerformed(boolean isCheckpointPerformed) {
			this.isCheckpointPerformed = isCheckpointPerformed;
		}

		public void increaseMsgCounterByOne() {
			this.msgCounter++;
		}

		public Message getLastMsgSent() {
			return lastMsgSent;
		}

		public void setLastMsgSent(Message lastMsgSent) {
			
			try {
				if(this.lastMsgSent != null && this.lastMsgSent.getJMSTimestamp() != lastMsgSent.getJMSTimestamp()) {
					this.msgIDWIthSameTS.clear();
				}
				
				this.msgIDWIthSameTS.add(lastMsgSent.getJMSMessageID());
			} catch (JMSException e) {
				
			}		
			this.lastMsgSent = lastMsgSent;	
			
		}

		public int getMsgCounter() {
			return msgCounter;
		}
		
		public void acknowledgeMsg() throws JMSException {
			if(lastMsgSent != null) {
				lastMsgSent.acknowledge();
			}
		}
		
		public void reset() {
			lastMsgSent = null;
			msgCounter = 0;
			isCheckpointPerformed = false;
			msgIDWIthSameTS = new ArrayList<String>();
		}
		
	}
	
	public String getConnectionDocument() {
		
		if (connectionDocument == null)
		{
			connectionDocument = getOperatorContext().getPE().getApplicationDirectory() + "/etc/connections.xml";
		}
		
		// if relative path, convert to absolute path
		if (!connectionDocument.startsWith("/"))
		{
			connectionDocument = getOperatorContext().getPE().getApplicationDirectory() + File.separator + connectionDocument;
		}
		
		return connectionDocument;
	}

	// Add the context checks

	@ContextCheck(compile = true)
	public static void checkInConsistentRegion(OperatorContextChecker checker) {
		ConsistentRegionContext consistentRegionContext = checker.getOperatorContext().getOptionalContext(ConsistentRegionContext.class);
		OperatorContext context = checker.getOperatorContext();
		
		if(consistentRegionContext != null && consistentRegionContext.isTriggerOperator() && !context.getParameterNames().contains("triggerCount")) {
			checker.setInvalidContext("triggerCount parameter must be set when consistent region is configured to operator driven", new String[] {});
		}
	}
	
	/*
	 * The method checkErrorOutputPort validates that the stream on error output
	 * port contains the mandatory attribute of type rstring which will contain
	 * the error message.
	 */
	@ContextCheck
	public static void checkErrorOutputPort(OperatorContextChecker checker) {
		OperatorContext context = checker.getOperatorContext();
		// Check if the error port is defined or not
		if (context.getNumberOfStreamingOutputs() == 2) {
			StreamingOutput<OutputTuple> streamingOutputErrorPort = context
					.getStreamingOutputs().get(1);
			// The optional error output port can have only one attribute of
			// type rstring
			if (streamingOutputErrorPort.getStreamSchema().getAttributeCount() != 1) {
				logger.log(LogLevel.ERROR, "ERROR_PORT_ATTR_COUNT_MAX");
			}
			if (streamingOutputErrorPort.getStreamSchema().getAttribute(0)
					.getType().getMetaType() != Type.MetaType.RSTRING) {
				logger.log(LogLevel.ERROR, "ERROR_PORT_ATTR_TYPE");
			}

		}
	}

	/*
	 * The method checkParametersRuntime validates that the reconnection policy
	 * parameters are appropriate
	 */
	@ContextCheck(compile = false)
	public static void checkParametersRuntime(OperatorContextChecker checker) {

		OperatorContext context = checker.getOperatorContext();

		if ((context.getParameterNames().contains("reconnectionBound"))) { // reconnectionBound
			// reconnectionBound value should be non negative.
			if (Integer.parseInt(context
					.getParameterValues("reconnectionBound").get(0)) < 0) {

				logger.log(LogLevel.ERROR, "REC_BOUND_NEG");
				checker.setInvalidContext(
						"reconnectionBound value {0} should be zero or greater than zero  ",
						new String[] { context.getParameterValues(
								"reconnectionBound").get(0) });
			}
			if (context.getParameterNames().contains("reconnectionPolicy")) {
				// reconnectionPolicy can be either InfiniteRetry, NoRetry,
				// BoundedRetry
				ReconnectionPolicies reconPolicy = ReconnectionPolicies
						.valueOf(context
								.getParameterValues("reconnectionPolicy")
								.get(0).trim());
				// reconnectionBound can appear only when the reconnectionPolicy
				// parameter is set to BoundedRetry and cannot appear otherwise
				if (reconPolicy != ReconnectionPolicies.BoundedRetry) {
					logger.log(LogLevel.ERROR, "REC_BOUND_NOT_ALLOWED");
					checker.setInvalidContext(
							"reconnectionBound {0} can appear only when the reconnectionPolicy parameter is set to BoundedRetry and cannot appear otherwise ",
							new String[] { context.getParameterValues(
									"reconnectionBound").get(0) });

				}
			}
		}
		// If initDelay parameter os specified then its value should be non
		// negative.
		if (context.getParameterNames().contains("initDelay")) {
			if (Integer.valueOf(context.getParameterValues("initDelay").get(0)) < 0) {
				logger.log(LogLevel.ERROR, "INIT_DELAY_NEG");
				checker.setInvalidContext(
						"initDelay value {0} should be zero or greater than zero  ",
						new String[] { context.getParameterValues("initDelay")
								.get(0).trim() });
			}
		}
		
		if(context.getParameterNames().contains("triggerCount")) {
			if(Integer.valueOf(context.getParameterValues("triggerCount").get(0)) < 1) {
				logger.log(LogLevel.ERROR, "triggerCount should be greater than zero");
				checker.setInvalidContext(
						"triggerCount value {0} should be greater than zero  ",
						new String[] { context.getParameterValues("triggerCount")
								.get(0).trim() });
			}
		}
		
        if (checker.getOperatorContext().getParameterNames().contains("messageIDOutAttrName")) { //$NON-NLS-1$
    		
    		List<String> parameterValues = checker.getOperatorContext().getParameterValues("messageIDOutAttrName"); //$NON-NLS-1$
    		String outAttributeName = parameterValues.get(0);
	    	List<StreamingOutput<OutputTuple>> outputPorts = checker.getOperatorContext().getStreamingOutputs();
	    	if (outputPorts.size() > 0)
	    	{
	    		StreamingOutput<OutputTuple> outputPort = outputPorts.get(0);
	    		StreamSchema streamSchema = outputPort.getStreamSchema();
	    		boolean check = checker.checkRequiredAttributes(streamSchema, outAttributeName);
	    		if (check)
	    			checker.checkAttributeType(streamSchema.getAttribute(outAttributeName), MetaType.RSTRING);
	    	}
    	}
		
	}

	// add check for reconnectionPolicy is present if either period or
	// reconnectionBound is specified
	@ContextCheck(compile = true)
	public static void checkParameters(OperatorContextChecker checker) {
		checker.checkDependentParameters("period", "reconnectionPolicy");
		checker.checkDependentParameters("reconnectionBound",
				"reconnectionPolicy");
	}

	@Override
	public synchronized void initialize(OperatorContext context)
			throws ParserConfigurationException, InterruptedException,
			IOException, ParseConnectionDocumentException, SAXException,
			NamingException, ConnectionException, Exception {

		super.initialize(context);
		
		consistentRegionContext = context.getOptionalContext(ConsistentRegionContext.class);
		
		JmsClasspathUtil.setupClassPaths(context);
		
		if(consistentRegionContext != null) {
			crState = new JMSSourceCRState();
		}

		// create connection document parser object (which is responsible for
		// parsing the connection document)
		ConnectionDocumentParser connectionDocumentParser = new ConnectionDocumentParser();
		// check if the error output port is specified or not

		if (context.getNumberOfStreamingOutputs() == 2) {
			hasErrorPort = true;
			errorOutputPort = getOutput(1);
		}

		// set the data output port
		dataOutputPort = getOutput(0);

		StreamSchema streamSchema = getOutput(0).getStreamSchema();

		// check if connections file is valid, if any of the below checks fail,
		// the operator throws a runtime error and abort

		connectionDocumentParser.parseAndValidateConnectionDocument(
				getConnectionDocument(), connection, access, streamSchema, false, context.getPE().getApplicationDirectory());

		// codepage parameter can come only if message class is bytes

		if (connectionDocumentParser.getMessageType() != MessageClass.bytes
				&& context.getParameterNames().contains("codepage")) {
			throw new ParseConnectionDocumentException(
					"codepage appears only when the message class is bytes");
		}
		// populate the message type and isAMQ

		messageType = connectionDocumentParser.getMessageType();
		isAMQ = connectionDocumentParser.isAMQ();
		// parsing connection document is successful, we can go ahead and create
		// connection
		// isProducer, false implies Consumer
		jmsConnectionHelper = new JMSConnectionHelper(reconnectionPolicy,
				reconnectionBound, period, false, 0,
				0, connectionDocumentParser.getDeliveryMode(),
				nReconnectionAttempts, logger, (consistentRegionContext != null), messageSelector);
		jmsConnectionHelper.createAdministeredObjects(
				connectionDocumentParser.getInitialContextFactory(),
				connectionDocumentParser.getProviderURL(),
				connectionDocumentParser.getUserPrincipal(),
				connectionDocumentParser.getUserCredential(),
				connectionDocumentParser.getConnectionFactory(),
				connectionDocumentParser.getDestination(),
				null);

		// Create the appropriate JMS message handlers as specified by the
		// messageType.

		switch (connectionDocumentParser.getMessageType()) {
			case map:
				messageHandlerImpl = new MapMessageHandler(
						connectionDocumentParser.getNativeSchemaObjects());
				break;
			case stream:
				messageHandlerImpl = new StreamMessageHandler(
						connectionDocumentParser.getNativeSchemaObjects());
				break;
			case bytes:
				messageHandlerImpl = new BytesMessageHandler(
						connectionDocumentParser.getNativeSchemaObjects(), codepage);
				break;
			case empty:
				messageHandlerImpl = new EmptyMessageHandler(
						connectionDocumentParser.getNativeSchemaObjects());
				break;
			case text:
				messageHandlerImpl = new TextMessageHandler(connectionDocumentParser.getNativeSchemaObjects());
				break;
			default:
				throw new RuntimeException("No valid message class is specified.");
		}
		
		// register for data governance
		registerForDataGovernance(connectionDocumentParser.getProviderURL(), connectionDocumentParser.getDestination());

	}

	private void registerForDataGovernance(String providerURL, String destination) {
		logger.log(TraceLevel.INFO, "JMSSource - Registering for data governance with providerURL: " + providerURL
				+ " destination: " + destination);
		DataGovernanceUtil.registerForDataGovernance(this, destination, IGovernanceConstants.ASSET_JMS_MESSAGE_TYPE,
				providerURL, IGovernanceConstants.ASSET_JMS_SERVER_TYPE, true, "JMSSource");
	}

	@Override
	protected void process() throws UnsupportedEncodingException,
			InterruptedException, ConnectionException, Exception {
		
		boolean isInConsistentRegion = consistentRegionContext != null;
		boolean isTriggerOperator = isInConsistentRegion && consistentRegionContext.isTriggerOperator();
		
		int msgIDAttrIndex = -1;
		
		if(this.getMessageIDOutAttrName() != null) {
			StreamSchema streamSchema = getOutput(0).getStreamSchema();
			msgIDAttrIndex = streamSchema.getAttributeIndex(this.getMessageIDOutAttrName());
		}
		
		
		// create the initial connection.
	    try {
			jmsConnectionHelper.createInitialConnection();
			if(isInConsistentRegion) {
				notifyResetLock(true);
		    }
		} catch (Exception e1) {
			
			if(isInConsistentRegion) {
				notifyResetLock(false);
			}
			// Initial connection fails to be created.
			// throw the exception.
			throw e1;
		}

		long timeout = isInConsistentRegion ? JMSSource.RECEIVE_TIMEOUT : 0;
		long sessionCreationTime = 0;

		while (!Thread.interrupted()) {
			// read a message from the consumer
			
			try {
				
				if(isInConsistentRegion) {
					consistentRegionContext.acquirePermit();
					
					// A checkpoint has been made, thus acknowledging the last sent message
					if(crState.isCheckpointPerformed()) {
						
						try {
							crState.acknowledgeMsg();
						} catch (Exception e) {
							consistentRegionContext.reset();
						} finally {
							crState.reset();
						}
				
					}
				}

				Message m = jmsConnectionHelper.receiveMessage(timeout);
				
				if(m == null) {
					continue;
				}
				
				if(isInConsistentRegion) {
					// following section takes care of possible duplicate messages
					// i.e connection re-created due to failure causing unacknowledged message to be delivered again
					// we don't want to process duplicate messages again.
					if(crState.getLastMsgSent() == null) {
						sessionCreationTime = jmsConnectionHelper.getSessionCreationTime();
					}
					else {
						// if session has been re-created and message is duplicate,ignore
						if(jmsConnectionHelper.getSessionCreationTime() > sessionCreationTime && 
						   isDuplicateMsg(m, crState.getLastMsgSent().getJMSTimestamp(), crState.getMsgIDWIthSameTS())) {
						    logger.log(LogLevel.INFO, "Ignored duplicated message: " + m.getJMSMessageID());
							continue;
						}
					}
				}
				
				// nMessagesRead indicates the number of messages which we have
				// read from the JMS Provider successfully
				nMessagesRead.incrementValue(1);
				OutputTuple dataTuple = dataOutputPort.newTuple();
				
				// convert the message to the output Tuple using the appropriate
				// message handler
				MessageAction returnVal = messageHandlerImpl
						.convertMessageToTuple(m, dataTuple);
				
				// take an action based on the return type
				switch (returnVal) {
				// the message type is incorrect
				case DISCARD_MESSAGE_WRONG_TYPE:
					nMessagesDropped.incrementValue(1);

					logger.log(LogLevel.WARN, "DISCARD_WRONG_MESSAGE_TYPE",
							new Object[] { messageType });

					// If the error output port is defined, redirect the error
					// to error output port
					if (hasErrorPort) {
						sendOutputErrorMsg("Discarding message as it has the wrong type , Expected: "
								+ messageType);
					}
					break;
				// if unexpected end of message has been reached
				case DISCARD_MESSAGE_EOF_REACHED:
					nMessagesDropped.incrementValue(1);
					logger.log(LogLevel.WARN, "DISCARD_MSG_TOO_SHORT");
					// If the error output port is defined, redirect the error
					// to error output port
					if (hasErrorPort) {
						sendOutputErrorMsg("The message is too short, and was discarded.");
					}
					break;
				// Mesage is read-only
				case DISCARD_MESSAGE_UNREADABLE:
					nMessagesDropped.incrementValue(1);
					logger.log(LogLevel.WARN, "DISCARD_MSG_UNREADABLE");
					// If the error output port is defined, redirect the error
					// to error output port
					if (hasErrorPort) {
						sendOutputErrorMsg("An attempt was made to read a write-only message, and the message was discarded.");
					}
					break;
				case DISCARD_MESSAGE_MESSAGE_FORMAT_ERROR:
					nMessagesDropped.incrementValue(1);
					logger.log(LogLevel.WARN,
							"DISCARD_MESSAGE_MESSAGE_FORMAT_ERROR");
					// If the error output port is defined, redirect the error
					// to error output port
					if (hasErrorPort) {
						sendOutputErrorMsg("The type conversion was invalid. Hence the message was discarded.");
					}
					break;
				// the message was read successfully
				case SUCCESSFUL_MESSAGE:
					if(msgIDAttrIndex != -1 && m.getJMSMessageID() != null) {
						dataTuple.setObject(msgIDAttrIndex, new RString(m.getJMSMessageID()));
					}
					dataOutputPort.submit(dataTuple);
					break;
				}
				
				// set last processed message
				if(isInConsistentRegion) {
					crState.setLastMsgSent(m);
				}
				
				// If the consistent region is driven by operator, then
				// 1. increase message counter
				// 2. Call make consistent region if message counter reached the triggerCounter specified by user
			    if(isTriggerOperator) {
					crState.increaseMsgCounterByOne();
					
					if(crState.getMsgCounter() == getTriggerCount()){
						consistentRegionContext.makeConsistent();
					}
			    }
			
			} catch (Exception Ex) {

			} finally {
				if(consistentRegionContext != null) {
					consistentRegionContext.releasePermit();
				}
			}
		}
	}

	// Send the error message on to the error output port if one is specified
	private void sendOutputErrorMsg(String errorMessage) throws Exception {
		OutputTuple errorTuple = errorOutputPort.newTuple();
		String consolidatedErrorMessage = errorMessage;
		// set the error message
		errorTuple.setString(0, consolidatedErrorMessage);
		// submit the tuple.
		errorOutputPort.submit(errorTuple);
	}

	@Override
	public void shutdown() throws Exception {
		// close the connection.
		
		if (isAMQ) {
			super.shutdown();
			jmsConnectionHelper.closeConnection();
		} else {
			jmsConnectionHelper.closeConnection();
			super.shutdown();
		}

	}
	
	private boolean isInitialConnectionEstablished() throws InterruptedException {
		
		synchronized(resetLock) {
			if(initalConnectionEstablished) {
				return true;
			}
			
			resetLock.wait();
			return initalConnectionEstablished;
		}
	}
	
	private void notifyResetLock(boolean result) {
		if(consistentRegionContext != null) {
	    	synchronized(resetLock) {
		    	initalConnectionEstablished = result;
		    	resetLock.notifyAll();
		    }
	    }
	}

	@Override
	public void close() throws IOException {
		
	}

	@Override
	public void checkpoint(Checkpoint checkpoint) throws Exception {
		logger.log(LogLevel.INFO, "Checkpoint... ");
	 
		crState.setCheckpointPerformed(true);
		
		ObjectOutputStream stream = checkpoint.getOutputStream();
		
		stream.writeBoolean(crState.getLastMsgSent() != null);
		
		if(crState.getLastMsgSent() != null) {
			stream.writeLong(crState.getLastMsgSent().getJMSTimestamp());
			stream.writeObject(crState.getMsgIDWIthSameTS());
		}
		
	}

	@Override
	public void drain() throws Exception {
		logger.log(LogLevel.INFO, "Drain... ");
		
	}

	@SuppressWarnings("unchecked")
	@Override
	public void reset(Checkpoint checkpoint) throws Exception {
		logger.log(LogLevel.INFO, "Reset to checkpoint " + checkpoint.getSequenceId());
		
		if(!isInitialConnectionEstablished()) {
			throw new ConnectionException("Connection to JMS failed.");
		}
		
		// Reset consistent region variables and recover JMS session to make re-delivery of
		// unacknowledged message 
		jmsConnectionHelper.recoverSession();
				
		ObjectInputStream stream = checkpoint.getInputStream();
		boolean hasMsg = stream.readBoolean();
		
		if(hasMsg) {
			long lastSentMsgTS = stream.readLong();
			List<String> lastSentMsgIDs =  (List<String>) stream.readObject();
			
			deduplicateMsg(lastSentMsgTS, lastSentMsgIDs);
		}
		
		crState.reset();
	
	}
	
	private boolean isDuplicateMsg(Message msg, long lastSentMsgTs, List<String> lastSentMsgIDs) throws JMSException {
		boolean res = false;
		
		if(msg.getJMSTimestamp() < lastSentMsgTs) {
			res = true;
		}			
		else if(msg.getJMSTimestamp() == lastSentMsgTs) {
			
			if(lastSentMsgIDs.contains(msg.getJMSMessageID())) {
				res = true;
			}
			
		}
		
		return res;
		
	}
	
	private void deduplicateMsg(long lastSentMsgTs, List<String> lastSentMsgIDs) throws JMSException, ConnectionException, InterruptedException {
		logger.log(LogLevel.INFO, "Deduplicate messages...");
		
		boolean stop = false;
		
		while(!stop) {
			
			Message msg = jmsConnectionHelper.receiveMessage(JMSSource.RECEIVE_TIMEOUT);
			
			if(msg == null) {
				return;
			}
			
			if(isDuplicateMsg(msg, lastSentMsgTs, lastSentMsgIDs)) {
				msg.acknowledge();
				logger.log(LogLevel.INFO, "Ignored duplicated message: " + msg.getJMSMessageID());
			}
			else {
				jmsConnectionHelper.recoverSession();
				stop = true;
			}
			
		}
		
	}

	@Override
	public void resetToInitialState() throws Exception {
		logger.log(LogLevel.INFO, "Resetting to Initial...");
		
		if(!isInitialConnectionEstablished()) {
			throw new ConnectionException("Connection to JMS failed.");
		}
		
		jmsConnectionHelper.recoverSession();
		crState.reset();
	
	}

	@Override
	public void retireCheckpoint(long id) throws Exception {
		logger.log(LogLevel.INFO, "Retire checkpoint " + id);
        
	}
}
