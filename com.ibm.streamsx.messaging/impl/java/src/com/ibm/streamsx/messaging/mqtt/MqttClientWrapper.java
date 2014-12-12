/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
 
package com.ibm.streamsx.messaging.mqtt;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.ibm.streams.operator.log4j.LoggerNames;
import com.ibm.streams.operator.log4j.TraceLevel;

public class MqttClientWrapper implements MqttCallback {
	private static final int COMMAND_TIMEOUT = 5000;

	private static final Logger TRACE = Logger.getLogger(MqttAsyncClientWrapper.class);
	private static final Logger LOG = Logger.getLogger(LoggerNames.LOG_FACILITY + "." + MqttAsyncClientWrapper.class.getName()); //$NON-NLS-1$
	
	private static final String EMPTY_STR = ""; //$NON-NLS-1$
	
	private String brokerUri;
	private MqttClient mqttClient;
	private MqttConnectOptions conOpt;
	
	private String pendingBrokerUri = EMPTY_STR;
	
	private ArrayList<MqttCallback> callBackListeners;
	private long period = 5000;
	private int reconnectionBound = 5;
	private String clientID;
	private long commandTimeout;
 
	private boolean shutdown; 
	
	public MqttClientWrapper() {	

    	conOpt = new MqttConnectOptions();
    	conOpt.setCleanSession(true);
    	
    	callBackListeners = new ArrayList<MqttCallback>();
	}
	
	public void setKeepAliveInterval(int keepAliveInterval) {
		if(keepAliveInterval >= 0) {
			conOpt.setKeepAliveInterval(keepAliveInterval);
		}
	}

	public void setCommandTimeout(long commandTimeout) {
		this.commandTimeout = commandTimeout;
	}

	public void setClientID(String clientID) {
		this.clientID = clientID;
	}

	public void setUserID(String userID) {
		if(userID != null && userID.trim().length() > 0) {
			conOpt.setUserName(userID);
		}
	}
	
	public void setPassword(String password) {
		if(password != null && password.trim().length() > 0) {
			conOpt.setPassword(password.toCharArray());
		}
	}
	
	public void setBrokerUri(String brokerUri) throws URISyntaxException {
		
		TRACE.log(TraceLevel.DEBUG,"SetBrokerUri: " + brokerUri); //$NON-NLS-1$
	
		// default to tcp:// if no scheme is specified
		if (!brokerUri.startsWith("tcp://") && !brokerUri.startsWith("ssl://")) //$NON-NLS-1$ //$NON-NLS-2$
		{
			brokerUri = "tcp://" + brokerUri; //$NON-NLS-1$
		}
		
		this.brokerUri = brokerUri;
	}
	
	public String getBrokerUri() {
		return brokerUri;
	}
	
	public void connect()
	{
    	MemoryPersistence dataStore = new MemoryPersistence();

    	try {
	    
	    	String clientId = newClientId();

			mqttClient = new MqttClient(this.brokerUri,clientId, dataStore);
			mqttClient.setTimeToWait(COMMAND_TIMEOUT);
			
			TRACE.log(TraceLevel.DEBUG,"Connect: " + brokerUri); //$NON-NLS-1$
			
			mqttClient.connect(conOpt);
						
			mqttClient.setCallback(this);

		} catch (MqttException e) {
			e.printStackTrace();
			
			// TODO:  Log
			System.exit(1);
		}
	}
	
	synchronized public void connect(int reconnectionBound, long period) throws InterruptedException, MqttException {
		
		this.reconnectionBound = reconnectionBound;
		this.period = period;
		
		MemoryPersistence dataStore = new MemoryPersistence();
		
		String clientId = (this.clientID != null) ? this.clientID : newClientId();
		

		TRACE.log(TraceLevel.INFO, "[Connect:]" + brokerUri); //$NON-NLS-1$
		TRACE.log(TraceLevel.INFO, "[Connect:] reconnectBound:" + reconnectionBound); //$NON-NLS-1$
		TRACE.log(TraceLevel.INFO, "[Connect:] period:" + period); //$NON-NLS-1$
		
		String uriToConnect = brokerUri;
		mqttClient = new MqttClient(uriToConnect, clientId, dataStore);
		
		if(this.commandTimeout != IMqttConstants.UNINITIALIZED_COMMAND_TIMEOUT) {
			mqttClient.setTimeToWait(commandTimeout);
		}
		
		mqttClient.setCallback(this);

		if (reconnectionBound > 0) {
			// Bounded retry
			for (int i = 0; i < reconnectionBound && !shutdown; i++) {
				boolean success = doConnectToServer(i);				
				if (success)
					break;
				
				// sleep for period before retrying 
				Thread.sleep(period);
				
				if (isUriChanged(uriToConnect)){
					// URI has changed, abort retry
					break;
				}
			}				
		} else if (reconnectionBound == 0)
		{
			// no retry, so try to connect once
			doConnectToServer(0);
		}
		else {
			// Infinite retry
			for (int i = 0; !shutdown; i++) {
				boolean success = doConnectToServer(i);
				if (success)
					break;
				
				// sleep for period before retrying
				Thread.sleep(period);
				
				if (isUriChanged(uriToConnect)){
					// URI has changed, abort retry
					break;
				}
			}
		}
		if (mqttClient.isConnected()) {			
			TRACE.log(TraceLevel.INFO, "[Connect Success:]" + brokerUri); //$NON-NLS-1$			
			
			// clear when connected
			clearPendingBrokerUri();
		} else {
			throw new MqttClientConnectException("Unable to connect to server: " //$NON-NLS-1$
					+ brokerUri);
		}

	}

	private String newClientId() {
		return "streams" + System.nanoTime();		
	}

	/**
	 * Connect to server, retrun true if successful, false otherwise
	 * @param period
	 * @param i
	 * @return
	 * @throws InterruptedException
	 */
	private boolean doConnectToServer(int i) 
			throws InterruptedException {

		try {
			TRACE.log(TraceLevel.DEBUG, "[Connect:] " + brokerUri + " Attempt: " + i); //$NON-NLS-1$ //$NON-NLS-2$
			mqttClient.connect(conOpt);			

		} catch (MqttSecurityException e) {
			TRACE.log(TraceLevel.ERROR, Messages.getString("Error_MqttClientWrapper.0"), e); //$NON-NLS-1$
			LOG.log(TraceLevel.ERROR, Messages.getString("Error_MqttClientWrapper.0"), e); //$NON-NLS-1$
		} catch (MqttException e) {
			TRACE.log(TraceLevel.ERROR,Messages.getString("Error_MqttClientWrapper.0"), e); //$NON-NLS-1$
			LOG.log(TraceLevel.ERROR, Messages.getString("Error_MqttClientWrapper.0"), e); //$NON-NLS-1$
		}

		return mqttClient.isConnected(); 
	}

	/**
     * Publish / send a message to an MQTT server
     * @param topicName the name of the topic to publish to
     * @param qos the quality of service to delivery the message at (0,1,2)
     * @param payload the set of bytes to send to the MQTT server
	 * @throws MqttPersistenceException 
     * @throws MqttException
	 * @throws InterruptedException 
	 * @throws URISyntaxException 
     */
	  public void publish(String topicName, int qos, byte[] payload, boolean retain) throws MqttPersistenceException, MqttException {    	       	
	    	// Construct the message to send
	   		MqttMessage message = new MqttMessage(payload);
	    	message.setQos(qos);
	    	message.setRetained(retain);

	    	if (mqttClient != null && mqttClient.isConnected()) {
				mqttClient.publish(topicName, message);
	    	}
	    }
    
    public void subscribe(String[] topics, int[] qos) throws MqttException {
    	
    	if (topics.length != qos.length)
    	{
    		throw new RuntimeException(Messages.getString("Error_MqttClientWrapper.4")); //$NON-NLS-1$
    	}
    	
    	if (TRACE.getLevel() == TraceLevel.INFO)
    	{
	    	for (int i : qos) {
	    		String msg = "[Subscribe:] {0} qos: {1}"; //$NON-NLS-1$	    
	    		TRACE.log(TraceLevel.INFO, msg); 
			}
    	}
    	
    	mqttClient.subscribe(topics, qos);    	    	    
    }
    
    public void unsubscribe(String[] topics) throws MqttException {
    	if (TRACE.getLevel() == TraceLevel.INFO)
    	{
	    	String msg = "[Unsubscribe:] " + topics;    //$NON-NLS-1$
	    } 
    	
    	mqttClient.unsubscribe(topics);   	 
    }
	
    synchronized public void disconnect() throws MqttException
    {
    	if (mqttClient != null)
    	{
    		if (mqttClient.isConnected())
    		{
		    	TRACE.log(TraceLevel.INFO, "[Disconnect:] " + brokerUri); //$NON-NLS-1$
				mqttClient.disconnect();
    		}
    	}
    }
    
    public void addCallBack(MqttCallback callback)
    {
    	if(!callBackListeners.contains(callback)) {
    		callBackListeners.add(callback);
    	}
    }
    
    public void removeCallBack(MqttCallback callback)
    {
    	callBackListeners.remove(callback);
    }

	@Override
	public void connectionLost(Throwable cause) {
		
		TRACE.log(TraceLevel.WARN, "Connection Lost: " + brokerUri); //$NON-NLS-1$
		
		for (Iterator iterator = callBackListeners.iterator(); iterator.hasNext();) {
			MqttCallback callbackListener = (MqttCallback) iterator.next();
			callbackListener.connectionLost(cause);
		}		
	}

	@Override
	public void messageArrived(String topic, MqttMessage message)
			throws Exception {
		
		TRACE.log(TraceLevel.TRACE, "[Message Arrived:] topic: " + topic + " qos " + message.getQos() +  " message: " + message.getPayload()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		
		for (Iterator iterator = callBackListeners.iterator(); iterator.hasNext();) {
			MqttCallback callbackListener = (MqttCallback) iterator.next();
			callbackListener.messageArrived(topic, message);
		}		
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
		
		TRACE.log(TraceLevel.TRACE, "[Message Delivery Complete:] " + token.getMessageId()); //$NON-NLS-1$
		
		for (Iterator iterator = callBackListeners.iterator(); iterator.hasNext();) {
			MqttCallback callbackListener = (MqttCallback) iterator.next();
			callbackListener.deliveryComplete(token);
		}		
	}
	
	public void shutdown()
	{
		TRACE.log(TraceLevel.DEBUG, "[Shutdown async client]"); //$NON-NLS-1$
		shutdown = true;
	}
	
	public void setPendingBrokerUri(String pendingBrokerUri) {
		this.pendingBrokerUri = pendingBrokerUri;
	}
	
	public String getPendingBrokerUri() {
		return pendingBrokerUri;
	}
	
	public void clearPendingBrokerUri()
	{
		setPendingBrokerUri(EMPTY_STR);
	}
	
	public boolean isUriChanged(String uriToConnect)
	{
		if (getPendingBrokerUri().isEmpty())
			return false;
		else if (getPendingBrokerUri().equals(uriToConnect))
			return false;
		
		return true;
	}
	
	public void setReconnectionBound(int reconnectionBound) {
		this.reconnectionBound = reconnectionBound;
	}
	
	public void setPeriod(long period) {
		this.period = period;
	}
	
	public boolean isConnected()
	{
		if (mqttClient == null)
			return false;
		
		return mqttClient.isConnected();
	}
	
	public void setSslProperties(Properties sslProperties)
	{
		conOpt.setSSLProperties(sslProperties);
	}

}
