/*******************************************************************************
 * Licensed Materials - Property of IBM
 * Copyright IBM Corp. 2014
 * US Government Users Restricted Rights - Use, duplication or
 * disclosure restricted by GSA ADP Schedule Contract with
 * IBM Corp.
 *******************************************************************************/
 
package com.ibm.streamsx.messaging.mqtt;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.ibm.streams.operator.log4j.LoggerNames;
import com.ibm.streams.operator.log4j.TraceLevel;

public class MqttClientWrapper implements MqttCallback {
	private static final int COMMAND_TIMEOUT = 5000;

	private static final Logger TRACE = Logger.getLogger(MqttAsyncClientWrapper.class);
	private static final Logger LOG = Logger.getLogger(LoggerNames.LOG_FACILITY + "." + MqttAsyncClientWrapper.class.getName());
	
	private String brokerUri;
	private MqttClient mqttClient;
	private MqttConnectOptions conOpt;
	
	private ArrayList<MqttCallback> callBackListeners;
	private float period = 5000;
	private int reconnectionBound = 5;
 
	private boolean shutdown; 
	
	public MqttClientWrapper() {	

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
	
	synchronized public void connect(int reconnectionBound, float period) throws InterruptedException, MqttException {
		
		this.reconnectionBound = reconnectionBound;
		this.period = period;
		
		MemoryPersistence dataStore = new MemoryPersistence();

		String clientId = MqttAsyncClient.generateClientId();

		TRACE.log(TraceLevel.INFO, "[Connect:]" + brokerUri); //$NON-NLS-1$
		TRACE.log(TraceLevel.INFO, "[Connect:] reconnectBound:" + reconnectionBound); //$NON-NLS-1$
		TRACE.log(TraceLevel.INFO, "[Connect:] period:" + period); //$NON-NLS-1$
		
		mqttClient = new MqttClient(this.brokerUri, clientId, dataStore);

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

		try {
			TRACE.log(TraceLevel.DEBUG, "[Connect:] " + brokerUri + " Attempt: " + i); //$NON-NLS-1$ //$NON-NLS-2$
			mqttClient.connect(conOpt);			

		} catch (MqttSecurityException e) {
			TRACE.log(TraceLevel.ERROR, Messages.getString("MqttAsyncClientWrapper.9"), e); //$NON-NLS-1$
			LOG.log(TraceLevel.ERROR, Messages.getString("MqttAsyncClientWrapper.9"), e); //$NON-NLS-1$
		} catch (MqttException e) {
			TRACE.log(TraceLevel.ERROR,Messages.getString("MqttAsyncClientWrapper.10"), e); //$NON-NLS-1$
			LOG.log(TraceLevel.ERROR, Messages.getString("MqttAsyncClientWrapper.9"), e); //$NON-NLS-1$
		}

		return mqttClient.isConnected();
	}

	/**
     * Publish / send a message to an MQTT server
     * @param topicName the name of the topic to publish to
     * @param qos the quality of service to delivery the message at (0,1,2)
     * @param payload the set of bytes to send to the MQTT server
     * @throws MqttException
	 * @throws InterruptedException 
     */
    public void publish(String topicName, int qos, byte[] payload) throws MqttException, InterruptedException {    	       	
    	// Construct the message to send
   		MqttMessage message = new MqttMessage(payload);
    	message.setQos(qos);

    	if (mqttClient != null && mqttClient.isConnected()) {
    		try {
				mqttClient.publish(topicName, message);
			} catch (Exception e) {
				if (!mqttClient.isConnected())
				{
		    		// make sure this client is disconnected
		    		disconnect();
		    		
		    		connect(reconnectionBound, period);
		    		
		    		// publish
		    		if (mqttClient.isConnected())
		    		{
		    			mqttClient.publish(topicName, message);
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
    			mqttClient.publish(topicName, message);
    		}
    	}
    }
    
    public void subscribe(String[] topics, int[] qos) throws MqttException {
    	
    	if (topics.length != qos.length)
    	{
    		throw new RuntimeException("Number of topics must equal to number of qos entries."); //$NON-NLS-1$
    	}
    	
    	if (TRACE.getLevel() == TraceLevel.INFO)
    	{
	    	for (int i : qos) {
	    		String msg = Messages.getString("MqttAsyncClientWrapper.1", topics[i], qos[i]); //$NON-NLS-1$	    
	    		TRACE.log(TraceLevel.INFO, msg); 
			}
    	}
    	
    	mqttClient.subscribe(topics, qos);    	    	    
    }
	
    synchronized public void disconnect()
    {
    	TRACE.log(TraceLevel.INFO, "[Disconnect:] " + brokerUri); //$NON-NLS-1$
    	try {
			mqttClient.disconnect();			
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
		
		TRACE.log(TraceLevel.WARN, Messages.getString("MqttAsyncClientWrapper.0") + brokerUri); //$NON-NLS-1$
		
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
