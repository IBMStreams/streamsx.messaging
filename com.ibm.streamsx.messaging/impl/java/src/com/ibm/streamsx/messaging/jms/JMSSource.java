/*******************************************************************************
 * Copyright (C) 2013, 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.jms;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
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
import com.ibm.streamsx.messaging.common.PropertyProvider;

//The JMSSource operator converts a message JMS queue or topic to stream
public class JMSSource extends ProcessTupleProducer implements StateHandler{	

	private static final String CLASS_NAME = "com.ibm.streamsx.messaging.jms.JMSSource"; //$NON-NLS-1$

	/**
	 * Create a {@code Logger} specific to this class that will write to the SPL
	 * log facility as a child of the {@link LoggerNames#LOG_FACILITY}
	 * {@code Logger}. The {@code Logger} uses a
	 */
	private static final Logger logger = Logger.getLogger(LoggerNames.LOG_FACILITY
			+ "." + CLASS_NAME, "com.ibm.streamsx.messaging.jms.JMSMessages"); //$NON-NLS-1$ //$NON-NLS-2$
	private static final Logger tracer = Logger.getLogger(CLASS_NAME);
	
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
	private String codepage = "UTF-8"; //$NON-NLS-1$
	// This mandatory parameter access specifies access specification name.
	private String access = null;
	// This mandatory parameter connection specifies name of the connection
	// specification containing a JMS element
	private String connection = null;
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
	private ReconnectionPolicies reconnectionPolicy = ReconnectionPolicies.valueOf("BoundedRetry"); //$NON-NLS-1$
	
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
	
	private String jmsDestinationOutAttrName = null;
	private String jmsDeliveryModeOutAttrName = null;
	private String jmsExpirationOutAttrName = null;
	private String jmsPriorityOutAttrName = null;
	private String jmsMessageIDOutAttrName = null;
	private String jmsTimestampOutAttrName = null;
	private String jmsCorrelationIDOutAttrName = null;
	private String jmsReplyToOutAttrName = null;
	private String jmsTypeOutAttrName = null;
	private String jmsRedeliveredOutAttrName = null;
	 
	private static List<String> jmsHeaderValOutAttrNames = Arrays.asList("messageIDOutAttrName",
																		"jmsDestinationOutAttrName",
																		"jmsDeliveryModeOutAttrName",
																		"jmsExpirationOutAttrName", 
																		"jmsPriorityOutAttrName",
																		"jmsMessageIDOutAttrName",
																		"jmsTimestampOutAttrName", 
																		"jmsCorrelationIDOutAttrName",
																		"jmsReplyToOutAttrName", 
																		"jmsTypeOutAttrName", 
																		"jmsRedeliveredOutAttrName");
	
	private Object resetLock = new Object();
	
	 // application configuration name
    private String appConfigName = null;
    
    // user property name stored in application configuration
    private String userPropName = null;
    
    // password property name stored in application configuration
    private String passwordPropName = null;
    
    private String keyStore = null;
    
    private String trustStore = null;
    
    private String keyStorePassword = null;
    
    private String trustStorePassword = null;
    
    private boolean sslConnection;

    public boolean isSslConnection() {
		return sslConnection;
	}

    @Parameter(optional = true)
    public void setSslConnection(boolean sslConnection) {
		this.sslConnection = sslConnection;
	}

    public String getTrustStorePassword() {
		return trustStorePassword;
	}
    
    @Parameter(optional = true)
    public void setTrustStorePassword(String trustStorePassword) {
		this.trustStorePassword = trustStorePassword;
	}
    
    public String getTrustStore() {
		return trustStore;
	}
    
    @Parameter(optional = true)
    public void setTrustStore(String trustStore) {
		this.trustStore = trustStore;
	}
    
    public String getKeyStorePassword() {
		return keyStorePassword;
	}
    
    @Parameter(optional = true)
    public void setKeyStorePassword(String keyStorePassword) {
		this.keyStorePassword = keyStorePassword;
	}
    
    public String getKeyStore() {
		return keyStore;
	}
    
    @Parameter(optional = true)
    public void setKeyStore(String keyStore) {
		this.keyStore = keyStore;
	}    
    

	public String getAppConfigName() {
		return appConfigName;
	}
    
	@Parameter(optional = true)
	public void setAppConfigName(String appConfigName) {
		this.appConfigName = appConfigName;
	}

	public String getUserPropName() {
		return userPropName;
	}
    
	@Parameter(optional = true)
	public void setUserPropName(String userPropName) {
		this.userPropName = userPropName;
	}

	public String getPasswordPropName() {
		return passwordPropName;
	}
    
	@Parameter(optional = true)
	public void setPasswordPropName(String passwordPropName) {
		this.passwordPropName = passwordPropName;
	}

	public String getMessageIDOutAttrName() {
		return messageIDOutAttrName;
	}

	@Parameter(optional = true)
	public void setMessageIDOutAttrName(String messageIDOutAttrName) {
		this.messageIDOutAttrName = messageIDOutAttrName;
	}

	public String getJmsDestinationOutAttrName() {
		return jmsDestinationOutAttrName;
	}

	@Parameter(optional = true, description = "The name of the output attribute to store into the JMS Header information JMSDestination" )
	public void setJmsDestinationOutAttrName(String jmsDestinationOutAttrName) {
		this.jmsDestinationOutAttrName = jmsDestinationOutAttrName;
	}

	public String getJmsDeliveryModeOutAttrName() {
		return jmsDeliveryModeOutAttrName;
	}

	@Parameter(optional = true, description = "The name of the output attribute to store into the JMS Header information JMSDeliveryMode" )
	public void setJmsDeliveryModeOutAttrName(String jmsDeliveryModeOutAttrName) {
		this.jmsDeliveryModeOutAttrName = jmsDeliveryModeOutAttrName;
	}

	public String getJmsExpirationOutAttrName() {
		return jmsExpirationOutAttrName;
	}

	@Parameter(optional = true, description = "The name of the output attribute to store into the JMS Header information JMSExpiration" )
public void setJmsExpirationOutAttrName(String jmsExpirationOutAttrName) {
		this.jmsExpirationOutAttrName = jmsExpirationOutAttrName;
	}

	public String getJmsPriorityOutAttrName() {
		return jmsPriorityOutAttrName;
	}

	@Parameter(optional = true, description = "The name of the output attribute to store into the JMS Header information JMSPriority" )
	public void setJmsPriorityOutAttrName(String jmsPriorityOutAttrName) {
		this.jmsPriorityOutAttrName = jmsPriorityOutAttrName;
	}

	public String getJmsMessageIDOutAttrName() {
		return jmsMessageIDOutAttrName;
	}

	@Parameter(optional = true, description = "The name of the output attribute to store into the JMS Header information JMSMessageID" )
	public void setJmsMessageIDOutAttrName(String jmsMessageIDOutAttrName) {
		this.jmsMessageIDOutAttrName = jmsMessageIDOutAttrName;
	}

	public String getJmsTimestampOutAttrName() {
		return jmsTimestampOutAttrName;
	}

	@Parameter(optional = true, description = "The name of the output attribute to store into the JMS Header information JMSTimestamp" )
	public void setJmsTimestampOutAttrName(String jmsTimestampOutAttrName) {
		this.jmsTimestampOutAttrName = jmsTimestampOutAttrName;
	}

	public String getJmsCorrelationIDOutAttrName() {
		return jmsCorrelationIDOutAttrName;
	}

	@Parameter(optional = true, description = "The name of the output attribute to store into the JMS Header information JMSCorrelationID" )
	public void setJmsCorrelationIDOutAttrName(String jmsCorrelationIDOutAttrName) {
		this.jmsCorrelationIDOutAttrName = jmsCorrelationIDOutAttrName;
	}

	public String getJmsReplyToOutAttrName() {
		return jmsReplyToOutAttrName;
	}

	@Parameter(optional = true, description = "The name of the output attribute to store into the JMS Header information JMSReplyTo" )
	public void setJmsReplyToOutAttrName(String jmsReplyToOutAttrName) {
		this.jmsReplyToOutAttrName = jmsReplyToOutAttrName;
	}

	public String getJmsTypeOutAttrName() {
		return jmsTypeOutAttrName;
	}

	@Parameter(optional = true, description = "The name of the output attribute to store into the JMS Header information JMSType" )
	public void setJmsTypeOutAttrName(String jmsTypeOutAttrName) {
		this.jmsTypeOutAttrName = jmsTypeOutAttrName;
	}

	public String getJmsRedeliveredOutAttrName() {
		return jmsRedeliveredOutAttrName;
	}

	@Parameter(optional = true, description = "The name of the output attribute to store into the JMS Header information JMSRedelivered" )
	public void setJmsRedeliveredOutAttrName(String jmsRedeliveredOutAttrName) {
		this.jmsRedeliveredOutAttrName = jmsRedeliveredOutAttrName;
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
			connectionDocument = getOperatorContext().getPE().getApplicationDirectory() + "/etc/connections.xml"; //$NON-NLS-1$
		}
		
		// if relative path, convert to absolute path
		if (!connectionDocument.startsWith("/")) //$NON-NLS-1$
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
		
		if(consistentRegionContext != null && consistentRegionContext.isTriggerOperator() && !context.getParameterNames().contains("triggerCount")) { //$NON-NLS-1$
			checker.setInvalidContext(Messages.getString("TRIGGERCOUNT_PARAM_MUST_BE_SET_WHEN_CONSISTENT_REGION_IS_OPERATOR_DRIVEN"), new String[] {}); //$NON-NLS-1$
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
				logger.log(LogLevel.ERROR, "ERROR_PORT_ATTR_COUNT_MAX"); //$NON-NLS-1$
			}
			if (streamingOutputErrorPort.getStreamSchema().getAttribute(0)
					.getType().getMetaType() != Type.MetaType.RSTRING) {
				logger.log(LogLevel.ERROR, "ERROR_PORT_ATTR_TYPE"); //$NON-NLS-1$
			}

		}
	}

	/*
	 * The method checkParametersRuntime validates that the reconnection policy
	 * parameters are appropriate
	 */
	@ContextCheck(compile = false)
	public static void checkParametersRuntime(OperatorContextChecker checker) {
		
		tracer.log(TraceLevel.TRACE, "Begin checkParametersRuntime()"); //$NON-NLS-1$

		OperatorContext context = checker.getOperatorContext();

		if ((context.getParameterNames().contains("reconnectionBound"))) { // reconnectionBound //$NON-NLS-1$
			// reconnectionBound value should be non negative.
			if (Integer.parseInt(context
					.getParameterValues("reconnectionBound").get(0)) < 0) { //$NON-NLS-1$

				logger.log(LogLevel.ERROR, "REC_BOUND_NEG"); //$NON-NLS-1$
				checker.setInvalidContext(
						Messages.getString("PARAM_VALUE_SHOULD_BE_GREATER_OR_EQUAL_TO_ZERO"),	//$NON-NLS-1$
						new String[] { "reconnectionBound", context.getParameterValues("reconnectionBound").get(0) }); //$NON-NLS-1$
			}
			if (context.getParameterNames().contains("reconnectionPolicy")) { //$NON-NLS-1$
				// reconnectionPolicy can be either InfiniteRetry, NoRetry,
				// BoundedRetry
				ReconnectionPolicies reconPolicy = ReconnectionPolicies
						.valueOf(context
								.getParameterValues("reconnectionPolicy") //$NON-NLS-1$
								.get(0).trim());
				// reconnectionBound can appear only when the reconnectionPolicy
				// parameter is set to BoundedRetry and cannot appear otherwise
				if (reconPolicy != ReconnectionPolicies.BoundedRetry) {
					logger.log(LogLevel.ERROR, "REC_BOUND_NOT_ALLOWED");	//$NON-NLS-1$
					checker.setInvalidContext(
							Messages.getString("RECONNECTIONBOUND_CAN_APPEAR_ONLY_WHEN_RECONNECTIONPOLICY_IS_BOUNDEDRETRY"), //$NON-NLS-1$
							new String[] { context.getParameterValues("reconnectionBound").get(0) }); //$NON-NLS-1$

				}
			}
		}
		// If initDelay parameter os specified then its value should be non
		// negative.
		if (context.getParameterNames().contains("initDelay")) { //$NON-NLS-1$
			if (Integer.valueOf(context.getParameterValues("initDelay").get(0)) < 0) { //$NON-NLS-1$
				logger.log(LogLevel.ERROR, "INIT_DELAY_NEG"); //$NON-NLS-1$
				checker.setInvalidContext(
						Messages.getString("PARAM_VALUE_SHOULD_BE_GREATER_OR_EQUAL_TO_ZERO"), //$NON-NLS-1$
						new String[] { "initDelay", context.getParameterValues("initDelay").get(0).trim() }); //$NON-NLS-1$
			}
		}
		
		if(context.getParameterNames().contains("triggerCount")) { //$NON-NLS-1$
			if(Integer.valueOf(context.getParameterValues("triggerCount").get(0)) < 1) { //$NON-NLS-1$
				logger.log(LogLevel.ERROR, "TRIGGERCOUNT_VALUE_SHOULD_BE_GREATER_THAN_ZERO", new String[] { context.getParameterValues("triggerCount").get(0).trim() } ); //$NON-NLS-1$ //$NON-NLS-2$
				checker.setInvalidContext(
						Messages.getString("TRIGGERCOUNT_VALUE_SHOULD_BE_GREATER_THAN_ZERO"), //$NON-NLS-1$
						new String[] { context.getParameterValues("triggerCount").get(0).trim() }); //$NON-NLS-1$
			}
		}
		
		for(String jmsHeaderValOutAttrName : jmsHeaderValOutAttrNames ) {
	        if (checker.getOperatorContext().getParameterNames().contains(jmsHeaderValOutAttrName)) { 
	    		
	    		List<String> parameterValues = checker.getOperatorContext().getParameterValues(jmsHeaderValOutAttrName); 
	    		String outAttributeName = parameterValues.get(0);
		    	List<StreamingOutput<OutputTuple>> outputPorts = checker.getOperatorContext().getStreamingOutputs();
		    	if (outputPorts.size() > 0)
		    	{
		    		StreamingOutput<OutputTuple> outputPort = outputPorts.get(0);
		    		StreamSchema streamSchema = outputPort.getStreamSchema();
		    		boolean check = checker.checkRequiredAttributes(streamSchema, outAttributeName);
		    		if (check) {
		    			switch (jmsHeaderValOutAttrName) {
		    			case "jmsDeliveryModeOutAttrName":
		    			case "jmsPriorityOutAttrName":
		    				checker.checkAttributeType(streamSchema.getAttribute(outAttributeName), MetaType.INT32);
		    				break;
		    			case "jmsExpirationOutAttrName":
		    			case "jmsTimestampOutAttrName":
		    				checker.checkAttributeType(streamSchema.getAttribute(outAttributeName), MetaType.INT64);
		    				break;
		    			case "jmsRedeliveredOutAttrName":
		    				checker.checkAttributeType(streamSchema.getAttribute(outAttributeName), MetaType.BOOLEAN);
		    				break;
		    			default:
		    				checker.checkAttributeType(streamSchema.getAttribute(outAttributeName), MetaType.RSTRING);
		    			}
		    		}
		    	}
	    	}
		}
        
        if((checker.getOperatorContext().getParameterNames().contains("appConfigName"))) { //$NON-NLS-1$
        	String appConfigName = checker.getOperatorContext().getParameterValues("appConfigName").get(0); //$NON-NLS-1$
			String userPropName = checker.getOperatorContext().getParameterValues("userPropName").get(0); //$NON-NLS-1$
			String passwordPropName = checker.getOperatorContext().getParameterValues("passwordPropName").get(0); //$NON-NLS-1$
			
			
			PropertyProvider provider = new PropertyProvider(checker.getOperatorContext().getPE(), appConfigName);
			
			String userName = provider.getProperty(userPropName, false);
			String password = provider.getProperty(passwordPropName, false);
			
			if(userName == null || userName.trim().length() == 0) {
				logger.log(LogLevel.ERROR, "PROPERTY_NOT_FOUND_IN_APP_CONFIG", new String[] {userPropName, appConfigName}); //$NON-NLS-1$
				checker.setInvalidContext(
						Messages.getString("PROPERTY_NOT_FOUND_IN_APP_CONFIG"), //$NON-NLS-1$
						new Object[] {userPropName, appConfigName});
			}
			
			if(password == null || password.trim().length() == 0) {
				logger.log(LogLevel.ERROR, "PROPERTY_NOT_FOUND_IN_APP_CONFIG", new String[] {passwordPropName, appConfigName}); //$NON-NLS-1$
				checker.setInvalidContext(
						Messages.getString("PROPERTY_NOT_FOUND_IN_APP_CONFIG"), //$NON-NLS-1$
						new Object[] {passwordPropName, appConfigName});
			}
        }
        
        tracer.log(TraceLevel.TRACE, "End checkParametersRuntime()"); //$NON-NLS-1$		
	}

	// add check for reconnectionPolicy is present if either period or
	// reconnectionBound is specified
	@ContextCheck(compile = true)
	public static void checkParameters(OperatorContextChecker checker) {
		checker.checkDependentParameters("period", "reconnectionPolicy"); //$NON-NLS-1$ //$NON-NLS-2$
		checker.checkDependentParameters("reconnectionBound", "reconnectionPolicy"); //$NON-NLS-1$ //$NON-NLS-2$
		
		// Make sure if appConfigName is specified then both userPropName and passwordPropName are needed
		checker.checkDependentParameters("appConfigName", "userPropName", "passwordPropName"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		checker.checkDependentParameters("userPropName", "appConfigName", "passwordPropName"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		checker.checkDependentParameters("passwordPropName", "appConfigName", "userPropName"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	@Override
	public synchronized void initialize(OperatorContext context)
			throws ParserConfigurationException, InterruptedException,
			IOException, ParseConnectionDocumentException, SAXException,
			NamingException, ConnectionException, Exception {

		tracer.log(TraceLevel.TRACE, "Begin initialize()"); //$NON-NLS-1$
		
		tracer.log(TraceLevel.TRACE, "Calling super class initialization"); //$NON-NLS-1$
		super.initialize(context);
		tracer.log(TraceLevel.TRACE, "Returned from super class initialization"); //$NON-NLS-1$
		
		consistentRegionContext = context.getOptionalContext(ConsistentRegionContext.class);
		
		JmsClasspathUtil.setupClassPaths(context);
		
		// set SSL system properties
		if(isSslConnection()) {
			
			tracer.log(TraceLevel.TRACE, "Setting up SSL connection"); //$NON-NLS-1$

			if(context.getParameterNames().contains("keyStore"))
				System.setProperty("javax.net.ssl.keyStore", getAbsolutePath(getKeyStore()));				
			if(context.getParameterNames().contains("keyStorePassword"))
				System.setProperty("javax.net.ssl.keyStorePassword", getKeyStorePassword());				
			if(context.getParameterNames().contains("trustStore"))
				System.setProperty("javax.net.ssl.trustStore",  getAbsolutePath(getTrustStore()));			
			if(context.getParameterNames().contains("trustStorePassword"))
				System.setProperty("javax.net.ssl.trustStorePassword",  getTrustStorePassword());
		}
		
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

		connectionDocumentParser.parseAndValidateConnectionDocument(getConnectionDocument(),
																	connection,
																	access,
																	streamSchema,
																	false,
																	context.getPE().getApplicationDirectory());

		// codepage parameter can come only if message class is bytes
		if (connectionDocumentParser.getMessageType() != MessageClass.bytes &&
			context.getParameterNames().contains("codepage")) //$NON-NLS-1$
		{
			throw new ParseConnectionDocumentException(Messages.getString("CODEPAGE_APPEARS_ONLY_WHEN_MSG_CLASS_IS_BYTES")); //$NON-NLS-1$
		}

		// populate the message type and isAMQ
		messageType = connectionDocumentParser.getMessageType();
		isAMQ = connectionDocumentParser.isAMQ();

		// parsing connection document is successful,
		// we can go ahead and create connection
		// isProducer, false implies Consumer
        PropertyProvider propertyProvider = null;
		if(getAppConfigName() != null) {
			propertyProvider = new PropertyProvider(context.getPE(), getAppConfigName());
		}
		
		jmsConnectionHelper = new JMSConnectionHelper(connectionDocumentParser, reconnectionPolicy,
				reconnectionBound, period, false, 0, 0, nReconnectionAttempts, logger, (consistentRegionContext != null), 
				messageSelector, propertyProvider, userPropName , passwordPropName, null);

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
				throw new RuntimeException(Messages.getString("NO_VALID_MSG_CLASS_SPECIFIED")); //$NON-NLS-1$
		}
		
		// register for data governance
		registerForDataGovernance(connectionDocumentParser.getProviderURL(), connectionDocumentParser.getDestination());

		tracer.log(TraceLevel.TRACE, "End initialize()"); //$NON-NLS-1$
	}

	protected String getAbsolutePath(String filePath) {
		if(filePath == null) 
			return null;
		
		Path p = Paths.get(filePath);
		if(p.isAbsolute()) {
			return filePath;
		} else {
			File f = new File (getOperatorContext().getPE().getApplicationDirectory(), filePath);
			return f.getAbsolutePath();
		}
	}
	
	private void registerForDataGovernance(String providerURL, String destination) {
		logger.log(TraceLevel.INFO, "JMSSource - Registering for data governance with providerURL: " + providerURL //$NON-NLS-1$
				+ " destination: " + destination); //$NON-NLS-1$
		DataGovernanceUtil.registerForDataGovernance(this, destination, IGovernanceConstants.ASSET_JMS_MESSAGE_TYPE,
				providerURL, IGovernanceConstants.ASSET_JMS_SERVER_TYPE, true, "JMSSource"); //$NON-NLS-1$
	}

	/* (non-Javadoc)
	 * @see com.ibm.streams.operator.samples.patterns.ProcessTupleProducer#process()
	 */
	@Override
	protected void process() throws IOException, ConnectionException
	{
		boolean	isInConsistentRegion	= consistentRegionContext != null;
		boolean	isTriggerOperator		= isInConsistentRegion && consistentRegionContext.isTriggerOperator();
		
		// create the initial connection.
	    try
	    {
	    	jmsConnectionHelper.createInitialConnection();
			if(isInConsistentRegion) {
				notifyResetLock(true);
		    }
	    }
	    catch (InterruptedException ie)
	    {
	        return;
		}
	    catch (ConnectionException ce)
	    {
			
			if(isInConsistentRegion) {
				notifyResetLock(false);
			}
			// Initial connection fails to be created.
			// throw the exception.
			throw ce;
		}

		long timeout				= JMSSource.RECEIVE_TIMEOUT;
		long sessionCreationTime	= 0;

		while (!Thread.interrupted())
		{
			// read a message from the consumer
			try
			{
				if(isInConsistentRegion)
				{
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

				Message msg = null;
				try
				{
				    msg = jmsConnectionHelper.receiveMessage(timeout);
				}
				catch (ConnectionException | InterruptedException ceie)
				{
				    throw ceie;
				}
				catch (/*JMS*/Exception e)
				{
				    // receive retry after reconnection attempt failed
				    continue;
                }
				
				if (msg == null)
				{
					continue;
				}
				
				// here we definitely have a JMS message. If exceptions happen here, we lose a message.
				// nMessagesRead indicates the number of messages which we have
				// read from the JMS Provider successfully
				nMessagesRead.incrementValue(1);
				try
				{
    				if(isInConsistentRegion)
    				{
    					// following section takes care of possible duplicate messages
    					// i.e connection re-created due to failure causing unacknowledged message to be delivered again
    					// we don't want to process duplicate messages again.
    					if(crState.getLastMsgSent() == null)
    					{
    						sessionCreationTime = jmsConnectionHelper.getSessionCreationTime();
    					}
    					else
    					{
    						// if session has been re-created and message is duplicate,ignore
    						if(jmsConnectionHelper.getSessionCreationTime() > sessionCreationTime && 
    						   isDuplicateMsg(msg, crState.getLastMsgSent().getJMSTimestamp(), crState.getMsgIDWIthSameTS())) {
    						    logger.log(LogLevel.INFO, "IGNORED_DUPLICATED_MSG", msg.getJMSMessageID()); //$NON-NLS-1$
    							continue;
    						}
    					}
    				}
    				
    				
    				OutputTuple dataTuple = dataOutputPort.newTuple();
				
    				// convert the message to the output Tuple using the appropriate message handler
    				MessageAction returnVal = messageHandlerImpl.convertMessageToTuple(msg, dataTuple);
    				
    				// take an action based on the return type
    				switch (returnVal) {
    				// the message type is incorrect
    				case DISCARD_MESSAGE_WRONG_TYPE:
    					nMessagesDropped.incrementValue(1);
    
    					logger.log(LogLevel.WARN, "DISCARD_WRONG_MESSAGE_TYPE", //$NON-NLS-1$
    							new Object[] { messageType });
    
    					// If the error output port is defined, redirect the error
    					// to error output port
    					if (hasErrorPort) {
    						sendOutputErrorMsg(Messages.getString("DISCARD_WRONG_MESSAGE_TYPE", new Object[] { messageType })); //$NON-NLS-1$
    					}
    					break;
    				// if unexpected end of message has been reached
    				case DISCARD_MESSAGE_EOF_REACHED:
    					nMessagesDropped.incrementValue(1);
    					logger.log(LogLevel.WARN, "DISCARD_MSG_TOO_SHORT"); //$NON-NLS-1$
    					// If the error output port is defined, redirect the error
    					// to error output port
    					if (hasErrorPort) {
    						sendOutputErrorMsg(Messages.getString("DISCARD_MSG_TOO_SHORT")); //$NON-NLS-1$
    					}
    					break;
    				// Mesage is read-only
    				case DISCARD_MESSAGE_UNREADABLE:
    					nMessagesDropped.incrementValue(1);
    					logger.log(LogLevel.WARN, "DISCARD_MSG_UNREADABLE"); //$NON-NLS-1$
    					// If the error output port is defined, redirect the error
    					// to error output port
    					if (hasErrorPort) {
    						sendOutputErrorMsg(Messages.getString("DISCARD_MSG_UNREADABLE")); //$NON-NLS-1$
    					}
    					break;
    				case DISCARD_MESSAGE_MESSAGE_FORMAT_ERROR:
    					nMessagesDropped.incrementValue(1);
    					logger.log(LogLevel.WARN,
    							"DISCARD_MESSAGE_MESSAGE_FORMAT_ERROR"); //$NON-NLS-1$
    					// If the error output port is defined, redirect the error
    					// to error output port
    					if (hasErrorPort) {
    						sendOutputErrorMsg(Messages.getString("DISCARD_MESSAGE_MESSAGE_FORMAT_ERROR")); //$NON-NLS-1$
    					}
    					break;
    				// the message was read successfully
    				case SUCCESSFUL_MESSAGE:
    					handleJmsHeaderValues(msg, dataTuple);
    					dataOutputPort.submit(dataTuple);
    					break;
    				default:
    				    nMessagesDropped.incrementValue(1);
    				    tracer.log(LogLevel.WARN, "No explicit handling for enum " + returnVal.getDeclaringClass().getSimpleName() + " value "
    				            + returnVal + " implemented in switch statement.", "JMSSource");
                        logger.log(LogLevel.WARN, "DISCARD_MESSAGE_MESSAGE_VARIABLE", "unexpected return value: " + returnVal); //$NON-NLS-1$
                        // If the error output port is defined, redirect the error
                        // to error output port
                        if (hasErrorPort) {
                            sendOutputErrorMsg(Messages.getString("DISCARD_MESSAGE_MESSAGE_VARIABLE", "unexpected return value: " + returnVal)); //$NON-NLS-1$
                        }
    				}
    				
    				// set last processed message
    				if(isInConsistentRegion) {
    					crState.setLastMsgSent(msg);
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
				}
				catch (Exception /*UnsupportedEncodingException | JMSException*/ e)
				{
				    nMessagesDropped.incrementValue(1);
	                tracer.log(LogLevel.WARN, "failed to convert JMS message to tuple: " + e.toString(), "JMSSource");
	                logger.log(LogLevel.WARN, "DISCARD_MESSAGE_MESSAGE_VARIABLE", e.toString()); //$NON-NLS-1$
	                // If the error output port is defined, redirect the error
	                // to error output port
	                if (hasErrorPort) {
	                    sendOutputErrorMsg(Messages.getString("DISCARD_MESSAGE_MESSAGE_VARIABLE", e.toString()));
	                }
				}
			}
			catch (InterruptedException e) {
			    // Thread has been interrupted, interrupted state is set; we will leave the while loop - no further action
			}
			catch (IOException | ConnectionException e) {
			    // problem resetting CR --> throw
			    // final problem with connection
			    throw e;
            }
			catch (Exception e) {
                // failed to send output tuple
                nMessagesDropped.incrementValue(1);
                tracer.log (LogLevel.WARN, "failed to submit output tuple: " + e.toString(), "JMSSource");
            }
			finally {
				if(consistentRegionContext != null) {
					consistentRegionContext.releasePermit();
				}
			}
		}
	}


	/**
	 * Handles the values of the JMSHeader of the current message. It first checks
	 * if there is an attribute in the Stream to write the header value into.
	 *  
	 * @param msg			The current JMS message.
	 * @param outTuple		The output tuple.
	 * @throws JMSException 
	 */
	private void handleJmsHeaderValues(Message msg, OutputTuple outTuple) throws JMSException {

		int messageIDAttrIdx		= -1;
		int jmsDestinationAttrIdx	= -1;
		int jmsDeliveryModeAttrIdx	= -1;
		int jmsExpirationAttrIdx	= -1;
		int jmsPriorityAttrIdx		= -1;
		int jmsMessageIDAttrIdx		= -1;
		int jmsTimestampAttrIdx		= -1;
		int jmsCorrelationIDAttrIdx	= -1;
		int jmsReplyToAttrIdx		= -1;
		int jmsTypeAttrIdx			= -1;
		int jmsRedeliveredAttrIdx	= -1;
				
		
		StreamSchema streamSchema = getOutput(0).getStreamSchema();

		
		if(this.getMessageIDOutAttrName() != null) {
			messageIDAttrIdx = streamSchema.getAttributeIndex(this.getMessageIDOutAttrName());
		}
		if(messageIDAttrIdx != -1 && msg.getJMSMessageID() != null) {
			outTuple.setObject(messageIDAttrIdx, new RString(msg.getJMSMessageID()));
		}

		
		if(this.getJmsDestinationOutAttrName() != null) {
			jmsDestinationAttrIdx = streamSchema.getAttributeIndex(this.getJmsDestinationOutAttrName());
		}
		if(jmsDestinationAttrIdx != -1 && msg.getJMSDestination() != null) {
			outTuple.setObject(jmsDestinationAttrIdx, new RString(msg.getJMSDestination().toString()));
		}
		
		if(this.getJmsDeliveryModeOutAttrName() != null) {
			jmsDeliveryModeAttrIdx = streamSchema.getAttributeIndex(this.getJmsDeliveryModeOutAttrName());
		}
		if(jmsDeliveryModeAttrIdx != -1) {
			outTuple.setObject(jmsDeliveryModeAttrIdx, new Integer(msg.getJMSDeliveryMode()));
		}
		
		if(this.getJmsExpirationOutAttrName() != null) {
			jmsExpirationAttrIdx = streamSchema.getAttributeIndex(this.getJmsExpirationOutAttrName());
		}
		if(jmsExpirationAttrIdx != -1) {
			outTuple.setObject(jmsExpirationAttrIdx, new Long(msg.getJMSExpiration()));
		}
		
		if(this.getJmsPriorityOutAttrName() != null) {
			jmsPriorityAttrIdx = streamSchema.getAttributeIndex(this.getJmsPriorityOutAttrName());
		}
		if(jmsPriorityAttrIdx != -1) {
			outTuple.setObject(jmsPriorityAttrIdx, new Integer(msg.getJMSPriority()));
		}
		
		if(this.getJmsMessageIDOutAttrName() != null) {
			jmsMessageIDAttrIdx = streamSchema.getAttributeIndex(this.getJmsMessageIDOutAttrName());
		}
		if(jmsMessageIDAttrIdx != -1 && msg.getJMSMessageID() != null) {
			outTuple.setObject(jmsMessageIDAttrIdx, new RString(msg.getJMSMessageID()));
		}

		if(this.getJmsTimestampOutAttrName() != null) {
			jmsTimestampAttrIdx = streamSchema.getAttributeIndex(this.getJmsTimestampOutAttrName());
		}
		if(jmsTimestampAttrIdx != -1) {
			outTuple.setObject(jmsTimestampAttrIdx, new Long(msg.getJMSTimestamp()));
		}
		
		if(this.getJmsCorrelationIDOutAttrName() != null) {
			jmsCorrelationIDAttrIdx = streamSchema.getAttributeIndex(this.getJmsCorrelationIDOutAttrName());
		}
		if(jmsCorrelationIDAttrIdx != -1 && msg.getJMSCorrelationID() != null) {
			outTuple.setObject(jmsCorrelationIDAttrIdx, new RString(msg.getJMSCorrelationID()));
		}
		
		if(this.getJmsReplyToOutAttrName() != null) {
			jmsReplyToAttrIdx = streamSchema.getAttributeIndex(this.getJmsReplyToOutAttrName());
		}
		if(jmsReplyToAttrIdx != -1 && msg.getJMSReplyTo() != null) {
			outTuple.setObject(jmsReplyToAttrIdx, new RString(msg.getJMSReplyTo().toString()));
		}
				
		if(this.getJmsTypeOutAttrName() != null) {
			jmsTypeAttrIdx = streamSchema.getAttributeIndex(this.getJmsTypeOutAttrName());
		}
		if(jmsTypeAttrIdx != -1 && msg.getJMSType() != null) {
			outTuple.setObject(jmsTypeAttrIdx, new RString(msg.getJMSType()));
		}
		
		if(this.getJmsRedeliveredOutAttrName() != null) {
			jmsRedeliveredAttrIdx = streamSchema.getAttributeIndex(this.getJmsRedeliveredOutAttrName());
		}
		if(jmsRedeliveredAttrIdx != -1) {
			outTuple.setObject(jmsRedeliveredAttrIdx, new Boolean(msg.getJMSRedelivered()));
		}
	}
	

	// Send the error message on to the error output port if one is specified
	private void sendOutputErrorMsg(String errorMessage) {
		OutputTuple errorTuple = errorOutputPort.newTuple();
		String consolidatedErrorMessage = errorMessage;
		// set the error message
		errorTuple.setString(0, consolidatedErrorMessage);
		// submit the tuple.
		try {
		errorOutputPort.submit(errorTuple);
		} catch (Exception e) {
		    tracer.log (LogLevel.ERROR, "failed to submit error output tuple: " + e.toString(), "JMSSource");
		}
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
		logger.log(LogLevel.INFO, "CHECKPOINT", checkpoint.getSequenceId()); //$NON-NLS-1$
	 
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
		logger.log(LogLevel.INFO, "DRAIN"); //$NON-NLS-1$		
	}

	@SuppressWarnings("unchecked")
	@Override
	public void reset(Checkpoint checkpoint) throws Exception {
		logger.log(LogLevel.INFO, "RESET_CHECKPOINT", checkpoint.getSequenceId()); //$NON-NLS-1$
		
		if(!isInitialConnectionEstablished()) {
			throw new ConnectionException(Messages.getString("CONNECTION_TO_JMS_FAILED", new Object[]{})); //$NON-NLS-1$
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
		logger.log(LogLevel.INFO, "DEDUPLICATE_MESSAGES"); //$NON-NLS-1$
		
		boolean stop = false;
		
		while(!stop) {
			
			Message msg = jmsConnectionHelper.receiveMessage(JMSSource.RECEIVE_TIMEOUT);
			
			if(msg == null) {
				return;
			}
			
			if(isDuplicateMsg(msg, lastSentMsgTs, lastSentMsgIDs)) {
				msg.acknowledge();
				logger.log(LogLevel.INFO, "IGNORED_DUPLICATED_MSG", msg.getJMSMessageID()); //$NON-NLS-1$
			}
			else {
				jmsConnectionHelper.recoverSession();
				stop = true;
			}
			
		}
		
	}

	@Override
	public void resetToInitialState() throws Exception {
		logger.log(LogLevel.INFO, "RESET_TO_INITIAL_STATE"); //$NON-NLS-1$
		
		if(!isInitialConnectionEstablished()) {
			throw new ConnectionException(Messages.getString("CONNECTION_TO_JMS_FAILED", new Object[]{})); //$NON-NLS-1$
		}
		
		jmsConnectionHelper.recoverSession();
		crState.reset();
	
	}

	@Override
	public void retireCheckpoint(long id) throws Exception {
		logger.log(LogLevel.INFO, "RETIRE_CHECKPOINT", id);		 //$NON-NLS-1$
	}
}
