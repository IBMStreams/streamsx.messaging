/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.mqtt;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.ibm.streams.operator.log4j.LoggerNames;
import com.ibm.streams.operator.log4j.TraceLevel;
import com.ibm.streamsx.messaging.mqtt.Messages;


public class MqttAsyncClientWrapper  implements MqttCallback{
	
	private static final int COMMAND_TIMEOUT = 5000;

	private static final Logger TRACE = Logger.getLogger(MqttAsyncClientWrapper.class);
	private static final Logger LOG = Logger.getLogger(LoggerNames.LOG_FACILITY + "." + MqttAsyncClientWrapper.class.getName()); //$NON-NLS-1$
	
	private String brokerUri;
	private MqttAsyncClient mqttClient;
	private MqttConnectOptions conOpt;
	
	private ArrayList<MqttCallback> callBackListeners;
	private float period = 5000;
	private int reconnectionBound = 5;

	private boolean shutdown;
	
	public MqttAsyncClientWrapper() {	
		
    	conOpt = new MqttConnectOptions();
    	conOpt.setCleanSession(true);
    	conOpt.setKeepAliveInterval(0);
    	
    	callBackListeners = new ArrayList<MqttCallback>();
	}
	
	public void setBrokerUri(String brokerUri) throws URISyntaxException {
		
		TRACE.log(TraceLevel.DEBUG,"SetBrokerUri: " + brokerUri); //$NON-NLS-1$
	
		URI uri = new URI(brokerUri);
		
		// default to tcp:// if no scheme is specified
		if (uri.getScheme() == null)
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
	    
	    	String clientId = MqttAsyncClient.generateClientId();

			mqttClient = new MqttAsyncClient(this.brokerUri,clientId, dataStore);

			TRACE.log(TraceLevel.DEBUG,"Connect: " + brokerUri); //$NON-NLS-1$
			
			IMqttToken mqttToken = mqttClient.connect(conOpt);
			mqttToken.waitForCompletion(10000);
			if (mqttToken.isComplete())
			{
				if (mqttToken.getException() != null)
				{
					// TODO: retry
				}
			}
			mqttClient.setCallback(this);

		} catch (MqttException e) {
			e.printStackTrace();
			
			// TODO:  Log
			System.exit(1);
		}
	}
	
	public void connect(int reconnectionBound, float period) throws InterruptedException, MqttException {
		
		this.reconnectionBound = reconnectionBound;
		this.period = period;
		
		MemoryPersistence dataStore = new MemoryPersistence();

		String clientId = MqttAsyncClient.generateClientId();

		TRACE.log(TraceLevel.INFO, "[Connect:]" + brokerUri); //$NON-NLS-1$
		TRACE.log(TraceLevel.INFO, "[Connect:] reconnectBound:" + reconnectionBound); //$NON-NLS-1$
		TRACE.log(TraceLevel.INFO, "[Connect:] period:" + period); //$NON-NLS-1$
		
		// Construct a non-blocking MQTT client instance
		mqttClient = new MqttAsyncClient(this.brokerUri, clientId, dataStore);

		if (reconnectionBound >= 0) {
			for (int i = 0; i < reconnectionBound && !shutdown; i++) {
				boolean success = doConnectToServer(period, i);				
				if (success)
					break;
			}
		} else {
			// this will be an infinite loop
			for (int i = 0; !shutdown; i++) {
				boolean success = doConnectToServer(period, i);
				if (success)
					break;
			}
		}
		if (mqttClient.isConnected()) {
			mqttClient.setCallback(this);
		} else {
			throw new RuntimeException("Unable to connect to server: " //$NON-NLS-1$
					+ brokerUri);
		}

	}

	/**
	 * Connect to server, retrun true if successful, false otherwise
	 * @param period
	 * @param i
	 * @return
	 * @throws InterruptedException
	 */
	private boolean doConnectToServer(float period, int i)
			throws InterruptedException {
		IMqttToken mqttToken = null;
		try {
			TRACE.log(TraceLevel.DEBUG, "[Connect:] " + brokerUri + " Attempt: " + i); //$NON-NLS-1$ //$NON-NLS-2$
			mqttToken = mqttClient.connect(conOpt);
			mqttToken.waitForCompletion(COMMAND_TIMEOUT);

		} catch (MqttSecurityException e) {
			TRACE.log(TraceLevel.ERROR, Messages.getString("UNABLE_TO_CONNECT_TO_SERVER_CLIENT_WRAPPER"), e); //$NON-NLS-1$
			LOG.log(TraceLevel.ERROR, Messages.getString("UNABLE_TO_CONNECT_TO_SERVER_CLIENT_WRAPPER"), e); //$NON-NLS-1$
		} catch (MqttException e) {
			TRACE.log(TraceLevel.ERROR,Messages.getString("UNABLE_TO_CONNECT_TO_SERVER_CLIENT_WRAPPER"), e); //$NON-NLS-1$
			LOG.log(TraceLevel.ERROR, Messages.getString("UNABLE_TO_CONNECT_TO_SERVER_CLIENT_WRAPPER"), e); //$NON-NLS-1$
		}

		if (mqttToken.isComplete() && mqttToken.getException() == null
				&& mqttClient.isConnected()) {
			TRACE.log(TraceLevel.DEBUG, "[Connect:] Success: " + " Attempt: " + i); //$NON-NLS-1$ //$NON-NLS-2$
			return true;
		} else {
			TRACE.log(TraceLevel.DEBUG, "[Connect:] Fail: " + " Attempt: " + i + " Sleep (ms): " + period); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			Thread.sleep((long) period);
			return false;
		}
	}

	/**
     * Publish / send a message to an MQTT server
     * @param topicName the name of the topic to publish to
     * @param qos the quality of service to delivery the message at (0,1,2)
     * @param payload the set of bytes to send to the MQTT server
     * @throws MqttException
	 * @throws InterruptedException 
     */
    public void publish(String topicName, int qos, byte[] payload, boolean retain) throws MqttException, InterruptedException {    	       	
    	// Construct the message to send
   		MqttMessage message = new MqttMessage(payload);
    	message.setQos(qos);
    	message.setRetained(retain);

    	if (mqttClient != null && mqttClient.isConnected()) {
    		try {
				mqttClient.publish(topicName, message, null, null);
			} catch (Exception e) {
				if (!mqttClient.isConnected())
				{
		    		// make sure this client is disconnected
		    		disconnect();
		    		
		    		connect(reconnectionBound, period);
		    		
		    		// publish
		    		if (mqttClient.isConnected())
		    		{
		    			mqttClient.publish(topicName, message, null, null);
		    		}
				}
			}
    	}
    	else if (mqttClient != null){
    		// make sure this client is disconnected
    		disconnect();
    		
    		connect(reconnectionBound, period);
    		
    		// publish
    		if (mqttClient.isConnected())
    		{
    			mqttClient.publish(topicName, message, null, null);
    		}
    	}
    }
    
    public void subscribe(String[] topics, int[] qos) throws MqttException {
    	
    	if (topics.length != qos.length)
    	{
    		throw new RuntimeException(Messages.getString("NUMBER_OF_TOPICS_MUST_EQUAL_QOS_ENTRIES")); //$NON-NLS-1$
    	}
    	
    	if (TRACE.getLevel() == TraceLevel.INFO)
    	{
	    	for (int i : qos) {
	    		String msg = "Topic: " + topics[i] + " / qos: " + qos[i];	//$NON-NLS-1$	//$NON-NLS-2$	    
	    		TRACE.log(TraceLevel.INFO, msg); 
			}
    	}
    	
    	IMqttToken mqttToken = mqttClient.subscribe(topics, qos);    	
    	
    	mqttToken.waitForCompletion(COMMAND_TIMEOUT);
    	
    	if (mqttToken.isComplete())
    	{
    		if (mqttToken.getException() != null)
    		{
    			TRACE.log(TraceLevel.ERROR, "Topics Subscription failed.", mqttToken.getException()); //$NON-NLS-1$
    			throw mqttToken.getException();
    		}
    	}    	
    }
	
    public void disconnect()
    {
    	TRACE.log(TraceLevel.INFO, "[Disconnect:] " + brokerUri); //$NON-NLS-1$
    	try {
			IMqttToken mqttToken = mqttClient.disconnect();
			mqttToken.waitForCompletion(10000);
		} catch (MqttException e) {																												
			TRACE.log(TraceLevel.ERROR, e);
		}
    }
    
    public void addCallBack(MqttCallback callback)
    {
    	callBackListeners.add(callback);
    }
    
    public void removeCallBack(MqttCallback callback)
    {
    	callBackListeners.remove(callback);
    }

	@Override
	public void connectionLost(Throwable cause) {
		
		TRACE.log(TraceLevel.WARN, "Connection lost: " + brokerUri); //$NON-NLS-1$
		
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

}
