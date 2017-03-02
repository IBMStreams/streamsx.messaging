/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.mqtt;

import java.util.HashMap;



/**
 * Represent a client request
 *
 */
public class MqttClientRequest {
	
	private static final String MQTTSRC_TOPIC_REPLACE = "REPLACE"; //$NON-NLS-1$
	private static final String MQTTSRC_TOPIC_UPDATE = "UPDATE"; //$NON-NLS-1$
	private static final String MQTTSRC_TOPIC_REMOVE = "REMOVE"; //$NON-NLS-1$
	private static final String MQTTSRC_TOPIC_ADD = "ADD"; //$NON-NLS-1$
	
	public enum MqttClientRequestType {CONNECT, ADD_TOPICS, REMOVE_TOPICS, UPDATE_TOPICS, REPLACE_TOPICS};
	
	// mapping between SPL topic description action to MQTT client request
	private static HashMap<String, MqttClientRequestType> SPL_TO_REQTYPE_MAP = new HashMap<String, MqttClientRequest.MqttClientRequestType>();
	
	static {
		SPL_TO_REQTYPE_MAP.put(MqttClientRequest.MQTTSRC_TOPIC_ADD, MqttClientRequestType.ADD_TOPICS);
		SPL_TO_REQTYPE_MAP.put(MqttClientRequest.MQTTSRC_TOPIC_REMOVE, MqttClientRequestType.REMOVE_TOPICS);
		SPL_TO_REQTYPE_MAP.put(MqttClientRequest.MQTTSRC_TOPIC_UPDATE, MqttClientRequestType.UPDATE_TOPICS);
		SPL_TO_REQTYPE_MAP.put(MqttClientRequest.MQTTSRC_TOPIC_REPLACE, MqttClientRequestType.REPLACE_TOPICS);
	}
	
	private MqttClientRequestType reqType;
	private String serverUri;
	private String[] topics;
	private int qos;
	
	
	public MqttClientRequestType getReqType() {
		return reqType;
	}
	public MqttClientRequest setReqType(MqttClientRequestType reqType) {
		this.reqType = reqType;
		return this;
	}
	public String getServerUri() {
		return serverUri;
	}
	public MqttClientRequest setServerUri(String serverUri) {
		this.serverUri = serverUri;
		return this;
	}
	public String[] getTopics() {
		return topics;
	}
	public MqttClientRequest setTopics(String[] topics) {
		this.topics = topics;
		return this;
	}
	public int getQos() {
		return qos;
	}
	public MqttClientRequest setQos(int qos) {
		this.qos = qos;
		return this; 
	}

	public static MqttClientRequestType getRequestType(String splUpdateCode)
	{
		return SPL_TO_REQTYPE_MAP.get(splUpdateCode);
	}
	
}
