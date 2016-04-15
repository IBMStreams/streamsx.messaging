/*******************************************************************************
 * Copyright (C) 2013, 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.jms;

import com.ibm.streams.operator.metrics.Metric;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageFormatException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import com.ibm.streams.operator.logging.LogLevel;

/* This class contains all the connection related information, creating maintaining and closing a connection to the JMSProvider
 * Sending and Receiving JMS messages
 */
class JMSConnectionHelper {

	// variables required to create connection
	// connection factory
	private ConnectionFactory connFactory = null;
	// destination
	private Destination dest = null;
	// jndi context
	private Context jndiContext = null;
	// connection
	private Connection connect = null;
	// JMS message producer
	private MessageProducer producer = null;
	// JMS message consumer
	private MessageConsumer consumer = null;
	// JMS session
	private Session session = null;
	// The reconnection Policy specified by the user
	// defaults to bounderRetry.
	private final ReconnectionPolicies reconnectionPolicy;
	// ReconnectionBound
	private final int reconnectionBound;
	// Period
	private final double period;
	// Is is a producer(JMSSink) or Consumer(JMSSource)
	// set to true for JMSSink,false for JMSSource
	private final boolean isProducer;
	// the delivery mode
	private final String deliveryMode;
	// the metric which specifies the number of reconnection attempts
	// made in case of initial or transient connection failures.
	private Metric nReconnectionAttempts;
	// Metric to indicate the number of failed inserts to the JMS Provider by
	// JMSSink
	private Metric nFailedInserts;
	// userPrincipal and userCredential will be initialized by 
	// createAdministeredObjects and used for connection
	private String userPrincipal = null;
	private String userCredential = null;
	
	// Max number of retries on message send
	private final int maxMessageRetries;

	// Time to wait before try to resend failed message
	private final long messageRetryDelay;
	
	// Indicate message ack mode is client or not
	private final boolean useClientAckMode;
	
	// JMS message selector
	private String messageSelector;
	
	// Timestamp of session creation
	private long sessionCreationTime;
	
	// Consistent region destination object
	private Destination destCR = null;
	
	// message producer of the CR queue
	private MessageProducer producerCR = null;
	
	// message consumer of the CR queue
	private MessageConsumer consumerCR = null;

	private synchronized MessageConsumer getConsumerCR() {
		return consumerCR;
	}

	private synchronized void setConsumerCR(MessageConsumer consumerCR) {
		this.consumerCR = consumerCR;
	}

	// getter for CR queue producer
	private synchronized MessageProducer getProducerCR() {
		return producerCR;
	}

	// setter for CR queue producer
	private synchronized void setProducerCR(MessageProducer producer) {
		this.producerCR = producer;
	}

	public long getSessionCreationTime() {
		return sessionCreationTime;
	}

	private void setSessionCreationTime(long sessionCreationTime) {
		this.sessionCreationTime = sessionCreationTime;
	}

	// procedure to detrmine if there exists a valid connection or not
	private boolean isConnectValid() {
		if (connect != null)
			return true;
		return false;
	}

	// getter for consumer
	private synchronized MessageConsumer getConsumer() {
		return consumer;
	}

	// setter for consumer
	private synchronized void setConsumer(MessageConsumer consumer) {
		this.consumer = consumer;
	}

	// getter for producer
	private synchronized MessageProducer getProducer() {
		return producer;
	}

	// setter for producer
	private synchronized void setProducer(MessageProducer producer) {
		this.producer = producer;
	}

	// getter for session
	synchronized Session getSession() {
		return session;
	}

	// setter for session, synchnized to avoid concurrent access to session
	// object
	private synchronized void setSession(Session session) {
		this.session = session;
		this.setSessionCreationTime(System.currentTimeMillis());
	}

	// setter for connect
	// connect is thread safe.Hence not synchronizing.
	private void setConnect(Connection connect) {
		this.connect = connect;
	}

	// getter for connect
	private Connection getConnect() {
		return connect;
	}

	// logger to get error messages
	private Logger logger;

	// This constructor sets the parameters required to create a connection for
	// JMSSource
	JMSConnectionHelper(ReconnectionPolicies reconnectionPolicy,
			int reconnectionBound, double period, boolean isProducer,
			int maxMessageRetry, long messageRetryDelay,
			String deliveryMode, Metric nReconnectionAttempts, Logger logger, boolean useClientAckMode, String messageSelector) {
		this.reconnectionPolicy = reconnectionPolicy;
		this.reconnectionBound = reconnectionBound;
		this.period = period;
		this.isProducer = isProducer;
		this.deliveryMode = deliveryMode;
		this.logger = logger;
		this.nReconnectionAttempts = nReconnectionAttempts;
		this.maxMessageRetries = maxMessageRetry;
		this.messageRetryDelay = messageRetryDelay;
		this.useClientAckMode = useClientAckMode;
		this.messageSelector = messageSelector;
	}

	// This constructor sets the parameters required to create a connection for
	// JMSSink
	JMSConnectionHelper(ReconnectionPolicies reconnectionPolicy,
			int reconnectionBound, double period, boolean isProducer,
			int maxMessageRetry, long messageRetryDelay, String deliveryMode, 
			Metric nReconnectionAttempts, Metric nFailedInserts, Logger logger, boolean useClientAckMode, String msgSelectorCR) {
		this(reconnectionPolicy, reconnectionBound, period, isProducer,
			 maxMessageRetry, messageRetryDelay, deliveryMode, nReconnectionAttempts, logger, useClientAckMode, msgSelectorCR);
		this.nFailedInserts = nFailedInserts;

	}

	// Method to create the initial connection
	public void createInitialConnection() throws ConnectionException,
			InterruptedException {
		createConnection();
		return;
	}
	
	
	// Method to create initial connection without retry
	public void createInitialConnectionNoRetry() throws ConnectionException {
		createConnectionNoRetry();
	}

	// this subroutine creates the initial jndi context by taking the mandatory
	// and optional parameters

	public void createAdministeredObjects(String initialContextFactory,
			String providerURL, String userPrincipal, String userCredential,
			String connectionFactory, String destination, String destinationCR)
			throws NamingException {

		this.userPrincipal = userPrincipal;
		this.userCredential = userCredential;
		
		// Create a JNDI API InitialContext object if none exists
		// create a properties object and add all the mandatory and optional
		// parameter
		// required to create the jndi context as specified in connection
		// document
		Properties props = new Properties();
		props.put(Context.INITIAL_CONTEXT_FACTORY, initialContextFactory);
		props.put(Context.PROVIDER_URL, providerURL);

		// Add the optional elements

		if (userPrincipal != null && userCredential != null) {
			props.put(Context.SECURITY_PRINCIPAL, userPrincipal);
			props.put(Context.SECURITY_CREDENTIALS, userCredential);
		}

		// create the jndi context

		jndiContext = new InitialContext(props);

		// Look up connection factory and destination. If either does not exist,
		// exit, throws a NamingException if lookup fails

		connFactory = (ConnectionFactory) jndiContext.lookup(connectionFactory);
		dest = (Destination) jndiContext.lookup(destination);
		
		// Look up CR queue only for producer and when producer is in a CR
		if(this.isProducer && this.useClientAckMode) {
			destCR = (Destination) jndiContext.lookup(destinationCR);
		}

		return;
	}

	// this subroutine creates the connection, it always verifies if we have a
	// successfull existing connection before attempting to create one.
	private synchronized void createConnection() throws ConnectionException,
			InterruptedException {
		int nConnectionAttempts = 0;
		// Check if connection exists or not.
		if (!isConnectValid()) {

			// Delay in miliseconds as specified in period parameter
			final long delay = TimeUnit.MILLISECONDS.convert((long) period,
					TimeUnit.SECONDS);

			while (!Thread.interrupted()) {
				// make a call to connect subroutine to create a connection
				// for each unsuccesfull attempt increment the
				// nConnectionAttempts
				try {
					nConnectionAttempts++;
					if (connect(isProducer)) {
						// got a successfull connection,
						// come out of while loop.
						break;
					}

				} catch (InvalidSelectorException e) {
					throw new ConnectionException(
							"Connection to JMS failed. Invalid message selector");
				} catch (JMSException e) {
					logger.log(LogLevel.ERROR, "RECONNECTION_EXCEPTION",
							new Object[] { e.toString() });
					// Get the reconnectionPolicy
					// Apply reconnection policies if the connection was
					// unsuccessful

					if (reconnectionPolicy == ReconnectionPolicies.NoRetry) {
						// Check if ReconnectionPolicy is noRetry, then abort
						throw new ConnectionException(
								"Connection to JMS failed. Did not try to reconnect as the policy is noReconnect");
					}

					// Check if ReconnectionPolicy is BoundedRetry, then try
					// once in
					// interval defined by period till the reconnectionBound
					// If no ReconnectionPolicy is mentioned then also we have a
					// default value of reconnectionBound and period

					else if (reconnectionPolicy == ReconnectionPolicies.BoundedRetry
							&& nConnectionAttempts == reconnectionBound) {
						// Bounded number of retries has exceeded.
						throw new ConnectionException(
								"Connection to JMS failed. Bounded number of tries has exceeded");
					}
					// sleep for delay interval
					Thread.sleep(delay);
					// Incremet the metric nReconnectionAttempts
					nReconnectionAttempts.incrementValue(1);
				}

			}

		}
	}
	
	private synchronized void createConnectionNoRetry() throws ConnectionException {
		
		if (!isConnectValid()) {
			try {
				connect(isProducer);
			} catch (JMSException e) {
				logger.log(LogLevel.ERROR, "Connection to JMS failed", new Object[] { e.toString() });
				throw new ConnectionException(
						"Connection to JMS failed. Did not try to reconnect as the policy is reconnection policy does not apply here.");
			}
		}
	}

	// this subroutine creates the connection, producer and consumer, throws a
	// JMSException if it fails
	private boolean connect(boolean isProducer) throws JMSException {

		// Create connection.
		if (userPrincipal != null && !userPrincipal.isEmpty() && 
				userCredential != null && !userCredential.isEmpty() )
			setConnect(connFactory.createConnection(userPrincipal, userCredential));
		else
			setConnect(connFactory.createConnection());
		
		// Create session from connection; false means
		// session is not transacted.
		
		if(isProducer) {
			setSession(getConnect().createSession(this.useClientAckMode, Session.AUTO_ACKNOWLEDGE));
		}
		else {
			setSession(getConnect().createSession(false, (this.useClientAckMode) ? Session.CLIENT_ACKNOWLEDGE : Session.AUTO_ACKNOWLEDGE));
		}
		

		if (isProducer == true) {
			// Its JMSSink, So we will create a producer
			setProducer(getSession().createProducer(dest));
			
			if(useClientAckMode) {
				// Create producer/consumer of the CR queue
				setConsumerCR(getSession().createConsumer(destCR, messageSelector));
			    setProducerCR(getSession().createProducer(destCR));
			   
			    // Set time to live to 1 week for CR messages and delivery mode to persistent
			    getProducerCR().setTimeToLive(TimeUnit.MILLISECONDS.convert(7L, TimeUnit.DAYS));
			    getProducerCR().setDeliveryMode(DeliveryMode.PERSISTENT);
			    // start the connection
				getConnect().start();
			}
			
			// set the delivery mode if it is specified
			// default is non-persistent
			if (deliveryMode == null) {
				getProducer().setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			} else {
				if (deliveryMode.trim().toLowerCase().equals("non_persistent")) {
					getProducer().setDeliveryMode(DeliveryMode.NON_PERSISTENT);
				}

				if (deliveryMode.trim().toLowerCase().equals("persistent")) {
					getProducer().setDeliveryMode(DeliveryMode.PERSISTENT);
				}
			}

		} else {
			// Its JMSSource, So we will create a consumer
			setConsumer(getSession().createConsumer(dest, messageSelector));
			// start the connection
			getConnect().start();
		}
		// create connection is successful, return true
		return true;
	}

	// subroutine which on receiving a message, send it to the
	// destination,update the metric
	// nFailedInserts if the send fails

	boolean sendMessage(Message message) throws ConnectionException,
			InterruptedException {

		boolean res = false;
		int count = 0;
		
		do {
			
			try {
				
				// This is retry, wait before retry
				if(count > 0) {
					logger.log(LogLevel.INFO, "ATTEMPT_TO_RESEND_MESSAGE", new Object[] { count });
					// Wait for a while before next delivery attempt
					Thread.sleep(messageRetryDelay);
				}
				// try to send the message
				synchronized (getSession()) {
					getProducer().send(message);
					res = true;
					
				}
			}
			catch (JMSException e) {
				// error has occurred, log error and try sending message again
				logger.log(LogLevel.WARN, "ERROR_DURING_SEND", new Object[] { e.toString() });
				logger.log(LogLevel.INFO, "ATTEMPT_TO_RECONNECT");
				
				// Recreate the connection objects if we don't have any (this
				// could happen after a connection failure)
				setConnect(null);
				createConnection();
			}
			
			count++;
			
		} while(count < maxMessageRetries && !res);
		
		if(!res) {
			nFailedInserts.incrementValue(1);
		}
		
		return res;
	}

	// this subroutine receives messages from a message consumer
	// This method supports the receive method with timeout
	Message receiveMessage(long timeout) throws ConnectionException, InterruptedException,
			JMSException {
		try {
			// try to receive a message via blocking method
			synchronized (getSession()) {
				return (getConsumer().receive(timeout));
			}
		}
		catch (JMSException e) {
			// If the JMSSource operator was interrupted in middle
			if (e.toString().contains("java.lang.InterruptedException")) {
				throw new java.lang.InterruptedException();
			}
			// Recreate the connection objects if we don't have any (this
			// could happen after a connection failure)
			setConnect(null);
			logger.log(LogLevel.WARN, "ERROR_DURING_RECEIVE",
					new Object[] { e.toString() });
			logger.log(LogLevel.INFO, "ATTEMPT_TO_RECONNECT");
			createConnection();
			// retry to receive again
			
			// try to receive a message via blocking method
			synchronized (getSession()) {
				return (getConsumer().receive(timeout));
			}
			
		}
	}
	
	// Send message without retry in case of failure
	// i.e connection problems, this method raise the error back to caller.
	// No connection or message retry will be attempted.
	boolean sendMessageNoRetry(Message message) throws JMSException {
		
		boolean res = false;
		
		try {
				
			// try to send the message
			synchronized (getSession()) {
				getProducer().send(message);
				res = true;
					
			}
		}
		catch (JMSException e) {
			// error has occurred, log error and try sending message again
			logger.log(LogLevel.WARN, "ERROR_DURING_SEND", new Object[] { e.toString() });
			
			// If the exception is caused by message format, then we can return peacefully as connection is still good.
			if(!(e instanceof MessageFormatException)) {
				throw e;
			}
			
		}

		if(!res) {
			nFailedInserts.incrementValue(1);
		}
		
		return res;
	}
	
	// send a consistent region message to the consistent region queue
	void sendCRMessage(Message message) throws JMSException {
			
		synchronized (getSession()) {
			getProducerCR().send(message);
		}

	}
		
	// receive a message from consistent region queue
	Message receiveCRMessage(long timeout) throws JMSException {
		
		synchronized (getSession()) {
			return (getConsumerCR().receive(timeout));
		}
	}
	
	// Recovers session causing unacknowledged message to be re-delivered
	public void recoverSession() throws JMSException, ConnectionException, InterruptedException {

		try {
			synchronized (getSession()) {
				getSession().recover();
			}
		} catch (JMSException e) {
			
			logger.log(LogLevel.INFO, "ATTEMPT_TO_RECONNECT");
			setConnect(null);
			createConnection();
			
			synchronized (getSession()) {
				getSession().recover();
			}
			
		}
	}
	
	public void commitSession() throws JMSException {
		
		synchronized (getSession()) {
			getSession().commit();
		}
	}
	
	public void roolbackSession() throws JMSException {
		
		synchronized (getSession()) {
			getSession().rollback();
		}
	}

	// close the connection
	public void closeConnection() throws JMSException {

		if (getSession() != null) {
			getSession().close();
		}
		if (getConnect() != null) {
			getConnect().close();
		}
	}
}
