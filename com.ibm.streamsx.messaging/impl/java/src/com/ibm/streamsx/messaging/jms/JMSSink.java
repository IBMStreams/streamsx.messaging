/*******************************************************************************
 * Copyright (C) 2013, 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.messaging.jms;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.logging.Logger;

import javax.jms.Message;
import javax.naming.NamingException;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.xml.sax.SAXException;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.logging.LogLevel;
import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.model.CustomMetric;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streams.operator.state.StateHandler;
import com.ibm.streamsx.messaging.common.DataGovernanceUtil;
import com.ibm.streamsx.messaging.common.IGovernanceConstants;
import com.ibm.streamsx.messaging.common.PropertyProvider;


//The JMSSink operator publishes data from Streams to a JMS Provider queue or a topic.

public class JMSSink extends AbstractOperator implements StateHandler{

	private static final String CLASS_NAME = "com.ibm.streamsx.messaging.jms.JMSSink";

	/**
	 * Create a {@code Logger} specific to this class that will write to the SPL
	 * log facility as a child of the {@link LoggerNames#LOG_FACILITY}
	 * {@code Logger}. The {@code Logger} uses a
	 */
	private static Logger logger = Logger.getLogger(LoggerNames.LOG_FACILITY
			+ "." + CLASS_NAME, "com.ibm.streamsx.messaging.jms.JMSMessages");
	
	// property names used in message header
	public static final String OP_CKP_NAME_PROPERTITY = "StreamsOperatorCkpName";
	
	public static final String CKP_ID_PROPERTITY = "checkpointId";

	// Variables required by the optional error output port

	// hasErrorPort signifies if the operator has error port defined or not
	// assuming in the beginning that the operator does not have a error output
	// port by setting hasErrorPort to false
	// further down in the code, if the no of output ports is 2, we set to true
	// We send data to error ouput port only in case where hasErrorPort is set
	// to true which implies that the opeator instance has a error output port
	// defined.
	private boolean hasErrorPort = false;
	// Variable to specifiy if the error ouput port has the optional input port
	// tuple or not
	// set to false initially
	private boolean hasOptionalTupleInErrorPort = false;
	// Variable to specify error output port
	private StreamingOutput<OutputTuple> errorOutputPort;
	// Variable to hold the schema of the embedded tuple(if present) in the
	// error output port
	private StreamSchema embeddedSchema;

	// Create an instance of JMSConnectionhelper
	private JMSConnectionHelper jmsConnectionHelper;

	// Metrices
	// nTruncatedInserts: The number of tuples that had truncated attributes
	// while converting to a message
	// nFailedInserts: The number of failed inserts to the JMS Provider.
	// nReconnectionAttempts: The number of reconnection attempts made before a
	// successful connection.
	Metric nTruncatedInserts;
	Metric nFailedInserts;
	Metric nReconnectionAttempts;

	// Initialize the metrices
	@CustomMetric(kind = Metric.Kind.COUNTER)
	public void setnFailedInserts(Metric nFailedInserts) {
		this.nFailedInserts = nFailedInserts;
	}

	@CustomMetric(kind = Metric.Kind.COUNTER)
	public void setnTruncatedInserts(Metric nTruncatedInserts) {
		this.nTruncatedInserts = nTruncatedInserts;
	}

	@CustomMetric(kind = Metric.Kind.COUNTER)
	public void setnReconnectionAttempts(Metric nReconnectionAttempts) {
		this.nReconnectionAttempts = nReconnectionAttempts;
	}

	// Operator parameters

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
	// filepath ../etc/connections.xml (with respect to the data directory)
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
	
	// This optional parameter maxMessageSendRetries specifies the number of successive 
	// retry that will be attempted for a message in case of failure on message send.
	// Default is 0.
	private int maxMessageSendRetries = 0;

	// This optional parameter messageSendRetryDelay specifies the time to wait before 
	// next delivery attempt. Default is 0 milliseconds
	private long messageSendRetryDelay = 0;

	// Declaring the JMSMEssagehandler,
	private JMSMessageHandlerImpl mhandler;
	// Variable to define if the connection attempted to the JMSProvider is the
	// first one.
	private boolean isInitialConnection = true;
	
	// consistent region context
    private ConsistentRegionContext consistentRegionContext;
    
    // CR queue name for storing checkpoint information
    private String consistentRegionQueueName;
    
    // variable to keep track of last successful check point sequeuce id.
    private long lastSuccessfulCheckpointId = 0;
    
    // unique id to identify messages on CR queue
    private String operatorUniqueID;
    
    // application configuration name
    private String appConfigName;
    
    // user property name stored in application configuration
    private String userPropName;
    
    // password property name stored in application configuration
    private String passwordPropName;
    

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

	public String getConsistentRegionQueueName() {
		return consistentRegionQueueName;
	}

	@Parameter(optional = true)
	public void setConsistentRegionQueueName(String consistentRegionQueueName) {
		this.consistentRegionQueueName = consistentRegionQueueName;
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
	
	// Optional parameter maxMessageRetries
	@Parameter(optional = true)
	public void setMaxMessageSendRetries(int maxMessageSendRetries) {
		this.maxMessageSendRetries = maxMessageSendRetries;
	}

	// Optional parameter messageRetryDelay
	@Parameter(optional = true)
	public void setMessageSendRetryDelay(long messageSendRetryDelay) {
		this.messageSendRetryDelay = messageSendRetryDelay;
	}

	// Optional parameter connectionDocument
	@Parameter(optional = true, description="Connection document containing connection information to connect to messaging servers.  If not specified, the connection document is assumed to be application_dir/etc/connections.xml.")
	public void setConnectionDocument(String connectionDocument) {
		this.connectionDocument = connectionDocument;
	}
	
	public String getConnectionDocument() {
		
		if (connectionDocument == null)
		{
			connectionDocument = getOperatorContext().getPE().getApplicationDirectory() + "/etc/connections.xml";
		}
		
		if (!connectionDocument.startsWith("/"))
		{
			connectionDocument = getOperatorContext().getPE().getApplicationDirectory() + File.separator + connectionDocument;
		}
		
		return connectionDocument;
	}

	// Add the context checks

	/*
	 * The method checkErrorOutputPort validates that the stream on error output
	 * port contains the optional attribute of type which is the incoming tuple,
	 * and an rstring which will contain the error message in order.
	 */
	@ContextCheck
	public static void checkErrorOutputPort(OperatorContextChecker checker) {
		OperatorContext context = checker.getOperatorContext();
		// Check if the operator has an error port defined
		if (context.getNumberOfStreamingOutputs() == 1) {
			StreamingOutput<OutputTuple> streamingOutputPort = context
					.getStreamingOutputs().get(0);
			// The optional error output port can have no more than two
			// attributes.
			if (streamingOutputPort.getStreamSchema().getAttributeCount() > 2) {
				logger.log(LogLevel.ERROR, "ATMOST_TWO_ATTR");

			}
			// The optional error output port must have at least one attribute.
			if (streamingOutputPort.getStreamSchema().getAttributeCount() < 1) {
				logger.log(LogLevel.ERROR, "ATLEAST_ONE_ATTR");

			}
			// If only one attribute is specified, that attribute in the
			// optional error output port must be an rstring.
			if (streamingOutputPort.getStreamSchema().getAttributeCount() == 1) {
				if (streamingOutputPort.getStreamSchema().getAttribute(0)
						.getType().getMetaType() != Type.MetaType.RSTRING) {
					logger.log(LogLevel.ERROR, "ERROR_PORT_FIRST_ATTR_RSTRING");
				}
			}
			// If two attributes are specified, the first attribute in the
			// optional error output port must be a tuple.
			// and the second attribute must be rstring.
			if (streamingOutputPort.getStreamSchema().getAttributeCount() == 2) {
				if (streamingOutputPort.getStreamSchema().getAttribute(0)
						.getType().getMetaType() != Type.MetaType.TUPLE) {
					logger.log(LogLevel.ERROR, "ERROR_PORT_FIRST_ATTR_TUPLE");

				}
				if (streamingOutputPort.getStreamSchema().getAttribute(1)
						.getType().getMetaType() != Type.MetaType.RSTRING) {
					logger.log(LogLevel.ERROR, "ERROR_PORT_SECOND_ATTR_RSTRING");

				}
			}
		}
	}
	
	@ContextCheck(compile = true, runtime = false)
	public static void checkCompileTimeConsistentRegion(OperatorContextChecker checker) {
		ConsistentRegionContext consistentRegionContext = checker.getOperatorContext().getOptionalContext(ConsistentRegionContext.class);
		OperatorContext context = checker.getOperatorContext();
		
		if(consistentRegionContext != null) {
			
			if(consistentRegionContext.isStartOfRegion()) {
				checker.setInvalidContext("The following operator cannot be the start of a consistent region: JMSSink", new String[] {});
			}
			
			if(!context.getParameterNames().contains("consistentRegionQueueName")) {
				checker.setInvalidContext("consistentRegionQueueName parameter must be set when JMSSink is participating in a consistent region", new String[] {});
			}
			
			if(context.getParameterNames().contains("reconnectionPolicy") ||
			   context.getParameterNames().contains("reconnectionBound") || 
			   context.getParameterNames().contains("period") || 
			   context.getParameterNames().contains("maxMessageSendRetries") || 
			   context.getParameterNames().contains("messageSendRetryDelay")) {
				
				checker.setInvalidContext("Parameters 'reconnectionPolicy', 'reconnectionBound', 'period', 'maxMessageSendRetries' and 'messageSendRetryDelay' can not be specified when JMSSink is participating in a consistent region", new String[] {});
			}
		}
		else {
			
			if(context.getParameterNames().contains("consistentRegionQueueName")) {
				checker.setInvalidContext("consistentRegionQueueName parameter can not be specified when JMSSink is not participating in a consistent region", new String[] {});
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

		if ((context.getParameterNames().contains("reconnectionBound"))) {
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
		
		// maxMessageSendRetries can not be negative number if present
		if(context.getParameterNames().contains("maxMessageSendRetries")) {
			if(Integer.parseInt(context.getParameterValues("maxMessageSendRetries").get(0)) < 0) {
				logger.log(LogLevel.ERROR, "MESSAGE_RESEND_NEG", new Object[] {"maxMessageSendRetries"});
				checker.setInvalidContext(
						"maxMessageSendRetries value {0} should be zero or greater than zero", 
						new String[] {context.getParameterValues("maxMessageSendRetries").get(0)});
			}
			
		}
		
		// messageSendRetryDelay can not be negative number if present
		if(context.getParameterNames().contains("messageSendRetryDelay")) {
			if(Long.parseLong(context.getParameterValues("messageSendRetryDelay").get(0)) < 0) {
				logger.log(LogLevel.ERROR, "MESSAGE_RESEND_NEG", new Object[] {"messageSendRetryDelay"});
				checker.setInvalidContext(
						"messageSendRetryDelay value {0} should be zero or greater than zero", 
						new String[] {context.getParameterValues("messageSendRetryDelay").get(0)});
			}
		}
		
		// consistentRegionQueueName must not be null if present
		if(context.getParameterNames().contains("consistentRegionQueueName")) {
			
			String consistentRegionQueueName = context.getParameterValues("consistentRegionQueueName").get(0);
			if(consistentRegionQueueName == null || consistentRegionQueueName.trim().length() == 0) {
				logger.log(LogLevel.ERROR, "consistentRegionQueueName value must be a non-empty value");
				checker.setInvalidContext(
						"consistentRegionQueueName value must be a non-empty value", 
						new String[] {});
			}
		}
		
		if((checker.getOperatorContext().getParameterNames().contains("appConfigName"))) {
        	String appConfigName = checker.getOperatorContext().getParameterValues("appConfigName").get(0);
			String userPropName = checker.getOperatorContext().getParameterValues("userPropName").get(0);
			String passwordPropName = checker.getOperatorContext().getParameterValues("passwordPropName").get(0);
			
			
			PropertyProvider provider = new PropertyProvider(checker.getOperatorContext().getPE(), appConfigName);
			
			String userName = provider.getProperty(userPropName);
			String password = provider.getProperty(passwordPropName);
			
			if(userName == null || userName.trim().length() == 0) {
				logger.log(LogLevel.ERROR, "Property " + userPropName + " is not found in application configuration " + appConfigName);
				checker.setInvalidContext(
						"Property {0} is not found in application configuration {1}.",
						new Object[] {userPropName, appConfigName});
				
			}
			
			if(password == null || password.trim().length() == 0) {
				logger.log(LogLevel.ERROR, "Property " + passwordPropName + " is not found in application configuration " + appConfigName);
				checker.setInvalidContext(
						"Property {0} is not found in application configuration {1}.",
						new Object[] {passwordPropName, appConfigName});
			
			}
        }
	}

	// add compile time check for either period or reconnectionBound to be
	// present only when reconnectionPolicy is present
	// and messageRetryDelay to be present only when maxMessageRetriesis present
	@ContextCheck(compile = true)
	public static void checkParameters(OperatorContextChecker checker) {
		ConsistentRegionContext consistentRegionContext = checker.getOperatorContext().getOptionalContext(ConsistentRegionContext.class);
		
		if(consistentRegionContext == null) {
			checker.checkDependentParameters("period", "reconnectionPolicy");
			checker.checkDependentParameters("reconnectionBound",
					"reconnectionPolicy");
			
			checker.checkDependentParameters("maxMessageSendRetries", "messageSendRetryDelay");
			checker.checkDependentParameters("messageSendRetryDelay", "maxMessageSendRetries");
		}
		
		// Make sure if appConfigName is specified then both userPropName and passwordPropName are needed
		checker.checkDependentParameters("appConfigName", "userPropName", "passwordPropName");
		checker.checkDependentParameters("userPropName", "appConfigName", "passwordPropName");
		checker.checkDependentParameters("passwordPropName", "appConfigName", "userPropName");
	}

	@Override
	public synchronized void initialize(OperatorContext context)
			throws ParserConfigurationException, InterruptedException,
			IOException, ParseConnectionDocumentException, SAXException,
			NamingException, ConnectionException, Exception {
		super.initialize(context);
		
		JmsClasspathUtil.setupClassPaths(context);
		
		consistentRegionContext = context.getOptionalContext(ConsistentRegionContext.class);
		
		operatorUniqueID = context.getPE().getDomainId() + "_" + context.getPE().getInstanceId() + "_" + context.getPE().getJobId() + "_" + context.getName();
		String msgSelectorCR = (consistentRegionContext == null) ? null : JMSSink.OP_CKP_NAME_PROPERTITY + "=" + "'" + operatorUniqueID + "'";
		
		/*
		 * Set appropriate variables if the optional error output port is
		 * specified. Also set errorOutputPort to the output port at index 0
		 */
		if (context.getNumberOfStreamingOutputs() == 1) {
			hasErrorPort = true;
			errorOutputPort = getOutput(0);
			if (errorOutputPort.getStreamSchema().getAttributeCount() == 2) {
				embeddedSchema = errorOutputPort.newTuple().getTuple(0)
						.getStreamSchema();
				hasOptionalTupleInErrorPort = true;
			}
		}

		// check if connections file is valid
		StreamSchema streamSchema = getInput(0).getStreamSchema();

		ConnectionDocumentParser connectionDocumentParser = new ConnectionDocumentParser();

		connectionDocumentParser.parseAndValidateConnectionDocument(
				getConnectionDocument(), connection, access, streamSchema, true, context.getPE().getApplicationDirectory());

		// codepage parameter can come only if message class is bytes
		// Since the message class is extracted runtime during the parsing of
		// connections document in initialize function , we need to keep this
		// check here
		if (connectionDocumentParser.getMessageType() != MessageClass.bytes
				&& context.getParameterNames().contains("codepage")) {
			throw new ParseConnectionDocumentException(
					"codepage appears only when the message class is bytes");
		}
		
		PropertyProvider propertyProvider = null;
		
		if(getAppConfigName() != null) {
			propertyProvider = new PropertyProvider(context.getPE(), getAppConfigName());
		}

		// parsing connection document is successful, we can go ahead and create
		// connection
		// The jmsConnectionHelper will throw a runtime error and abort the
		// application in case of errors.
		jmsConnectionHelper = new JMSConnectionHelper(connectionDocumentParser, reconnectionPolicy,
				reconnectionBound, period, true, maxMessageSendRetries, 
				messageSendRetryDelay, nReconnectionAttempts, nFailedInserts, logger, (consistentRegionContext != null), 
				msgSelectorCR, propertyProvider, userPropName, passwordPropName, getConsistentRegionQueueName());
		
		// Initialize JMS connection if operator is in a consistent region.
		// When this operator is in a consistent region, a transacted session is used,
		// So the connection/message send retry logic does not apply, here the noRetry version
		// of the connection initialization method is used, thus it makes sure to do it in Initial method than in process method.
		if(consistentRegionContext != null) {
			jmsConnectionHelper.createInitialConnectionNoRetry();
		}

		// Create the appropriate JMS message handlers as specified by the
		// messageType.
		switch (connectionDocumentParser.getMessageType()) {
		case map:
			mhandler = new MapMessageHandler(
					connectionDocumentParser.getNativeSchemaObjects(),
					nTruncatedInserts);

			break;
		case stream:
			mhandler = new StreamMessageHandler(
					connectionDocumentParser.getNativeSchemaObjects(),
					nTruncatedInserts);

			break;
		case bytes:
			mhandler = new BytesMessageHandler(
					connectionDocumentParser.getNativeSchemaObjects(),
					codepage, nTruncatedInserts);

			break;
		case empty:
			mhandler = new EmptyMessageHandler(
					connectionDocumentParser.getNativeSchemaObjects());

			break;
		case wbe:
			mhandler = new WBETextMessageHandler(
					connectionDocumentParser.getNativeSchemaObjects(),
					getInput(0).getName(), nTruncatedInserts);

			break;
		case wbe22:
			mhandler = new WBE22TextMessageHandler(
					connectionDocumentParser.getNativeSchemaObjects(),
					getInput(0).getName(), nTruncatedInserts);

			break;
		case xml:
			mhandler = new XMLTextMessageHandler(
					connectionDocumentParser.getNativeSchemaObjects(),
					getInput(0).getName(), nTruncatedInserts);

			break;
		case text:
			mhandler = new TextMessageHandler(connectionDocumentParser.getNativeSchemaObjects());
			break;
		default:
			throw new RuntimeException("No valid message class is specified.");
		}

		// register for data governance
		registerForDataGovernance(connectionDocumentParser.getProviderURL(), connectionDocumentParser.getDestination());

	}

	private void registerForDataGovernance(String providerURL, String destination) {
		logger.log(TraceLevel.INFO, "JMSSink - Registering for data governance with providerURL: " + providerURL
				+ " destination: " + destination);
		DataGovernanceUtil.registerForDataGovernance(this, destination, IGovernanceConstants.ASSET_JMS_MESSAGE_TYPE,
				providerURL, IGovernanceConstants.ASSET_JMS_SERVER_TYPE, false, "JMSSink");
	}

	@Override
	public void process(StreamingInput<Tuple> stream, Tuple tuple)
			throws InterruptedException, ConnectionException,
			UnsupportedEncodingException, ParserConfigurationException,
			TransformerException, Exception {
		
		boolean msgSent = false;
		
		// Create the initial connection for the first time only
		// This is only called if the operator is NOT in a consistent region.
		if(consistentRegionContext == null) {
			if (isInitialConnection) {
				jmsConnectionHelper.createInitialConnection();	
				isInitialConnection = false;
			}	
		}
		
		// Construct the JMS message based on the message type taking the
		// attributes from the tuple.
		Message message = mhandler.convertTupleToMessage(tuple,
				jmsConnectionHelper.getSession());
		// Send the message
		// If an exception occured while sending , drop the particular tuple.
		
		if(consistentRegionContext == null) {
			// Operator is not in a consistent region
			msgSent = jmsConnectionHelper.sendMessage(message);
		}
		else {
			msgSent = jmsConnectionHelper.sendMessageNoRetry(message);
		}
		
		if (!msgSent) {
									
			logger.log(LogLevel.ERROR, "EXCEPTION_SINK");
			if (hasErrorPort) {
				sendOutputErrorMsg(tuple,
						"Dropping this tuple since an exception occurred while sending.");
			}
			
		}

	}

	// Method to send the error message to the error output port if one is
	// specified
	private void sendOutputErrorMsg(Tuple tuple, String errorMessage)
			throws Exception {

		OutputTuple errorTuple = errorOutputPort.newTuple();
		String consolidatedErrorMessage = errorMessage;
		// If the error output port speciifes the optional input tuple, populate
		// its elements from the input tuple which caused this error.
		if (hasOptionalTupleInErrorPort) {
			Tuple embeddedTuple = embeddedSchema.getTuple(tuple);
			errorTuple.setTuple(0, embeddedTuple);
			errorTuple.setString(1, consolidatedErrorMessage);
		} else {
			// Set only the error message
			errorTuple.setString(0, consolidatedErrorMessage);
		}
		// submit the tuple.
		errorOutputPort.submit(errorTuple);

	}

	@Override
	public void shutdown() throws Exception {
		// close the connection.
		super.shutdown();
		jmsConnectionHelper.closeConnection();
	}

	@Override
	public void close() throws IOException {
		logger.log(LogLevel.INFO, "StateHandler Close");
	}

	@Override
	public void checkpoint(Checkpoint checkpoint) throws Exception {
		logger.log(LogLevel.INFO, "checkpoint " + checkpoint.getSequenceId());
		
		long currentCheckpointId = checkpoint.getSequenceId();
		long committedCheckpointId = 0;
		boolean commit = true;
		
		
		// The entire checkpoint should fail if any of the JMS operation fails.
		// For example, if connection drops at receiveMssage, this means the tuples sent
		// before current checkpoint are lost because we are using transacted session for CR
		// It is not necessary to try to establish the connection again,
		// any JMSException received should be propagated back to the CR framework.
			
		// retrieve a message from CR queue
		Message crMsg = jmsConnectionHelper.receiveCRMessage(500);

		// No checkpoint message yet, it may happen for the first checkpoint
		if(crMsg != null) {
		    committedCheckpointId = crMsg.getLongProperty(JMSSink.CKP_ID_PROPERTITY);
		}
			
		// Something is wrong here as committedCheckpointId should be greater than 1 when a successful checkpoint has been made
		if(committedCheckpointId == 0 && lastSuccessfulCheckpointId > 0) {
			logger.log(LogLevel.ERROR, "Checkpoint can not proceed, expecting a checkpoint message, but not found.");
		    throw new Exception("Checkpoint can not proceed, expecting a checkpoint message, but not found.");
		}
			
		if((currentCheckpointId - lastSuccessfulCheckpointId > 1) &&
		   (lastSuccessfulCheckpointId < committedCheckpointId)) {
			// this transaction has been processed before, and this is a duplicate
			// discard this transaction. 
			logger.log(LogLevel.INFO, "discard this transaction as it has been processed last time.");
		    commit = false;
				
		}
			
		if(commit) {
			jmsConnectionHelper.sendCRMessage(createCheckpointMsg(currentCheckpointId));
			jmsConnectionHelper.commitSession();
		}
		else {
			jmsConnectionHelper.roolbackSession();
		}
			
		lastSuccessfulCheckpointId = currentCheckpointId;
			
	}
	
	private Message createCheckpointMsg(long currentCheckpointId) throws Exception {
		
		Message message;
		message = jmsConnectionHelper.getSession().createMessage();
		message.setStringProperty(JMSSink.OP_CKP_NAME_PROPERTITY, operatorUniqueID);
		message.setLongProperty(JMSSink.CKP_ID_PROPERTITY, currentCheckpointId);
		
		return message;
	}

	@Override
	public void drain() throws Exception {
		logger.log(LogLevel.INFO, "Drain... ");
	}

	@Override
	public void reset(Checkpoint checkpoint) throws Exception {
		logger.log(LogLevel.INFO, "Reset to checkpoint " + checkpoint.getSequenceId());
	
		lastSuccessfulCheckpointId = checkpoint.getSequenceId();
		jmsConnectionHelper.roolbackSession();
		
	}

	@Override
	public void resetToInitialState() throws Exception {
		logger.log(LogLevel.INFO, "Reset to initial state");
		
		lastSuccessfulCheckpointId = 0;
		jmsConnectionHelper.roolbackSession();
	}

	@Override
	public void retireCheckpoint(long id) throws Exception {
		logger.log(LogLevel.INFO, "Retire checkpoint");		
	}
}
