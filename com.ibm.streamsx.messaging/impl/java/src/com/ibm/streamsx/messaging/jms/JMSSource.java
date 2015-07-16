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

import org.xml.sax.SAXException;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.logging.LogLevel;
import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.model.CustomMetric;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.samples.patterns.ProcessTupleProducer;
import com.ibm.streams.operator.state.ConsistentRegionContext;

//The JMSSource operator converts a message JMS queue or topic to stream
public class JMSSource extends ProcessTupleProducer {	

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

	// Variables to hold performance metrices for JMSSource

	// nMessagesRead is the number of messages read successfully.
	// nMessagesDropped is the number of messages dropped.
	// nReconnectionAttempts is the number of reconnection attempts made before
	// a successful connection.

	Metric nMessagesRead;
	Metric nMessagesDropped;
	Metric nReconnectionAttempts;

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
		
		if(consistentRegionContext != null) {
			checker.setInvalidContext("The following operator cannot be in a consistent region: JMSSource", new String[] {});
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
		
		JmsClasspathUtil.setupClassPaths(context);

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
				nReconnectionAttempts, logger);
		jmsConnectionHelper.createAdministeredObjects(
				connectionDocumentParser.getInitialContextFactory(),
				connectionDocumentParser.getProviderURL(),
				connectionDocumentParser.getUserPrincipal(),
				connectionDocumentParser.getUserCredential(),
				connectionDocumentParser.getConnectionFactory(),
				connectionDocumentParser.getDestination());

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
	}

	@Override
	protected void process() throws UnsupportedEncodingException,
			InterruptedException, ConnectionException, Exception {
		// create the initial connection.
		jmsConnectionHelper.createInitialConnection();
		while (!Thread.interrupted()) {
			// read a message from the consumer
			try {
				Message m = jmsConnectionHelper.receiveMessage();
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
					dataOutputPort.submit(dataTuple);
					break;
				}

			} catch (Exception Ex) {

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
}
