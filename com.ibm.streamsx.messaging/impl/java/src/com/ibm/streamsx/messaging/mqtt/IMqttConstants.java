/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.mqtt;

public interface IMqttConstants {

	public static final String CONN_TRUSTSTORE = "connection.trustStore";
	public static final String CONN_KEYSTORE = "connection.keyStore";
	public static final String CONN_KEYSTORE_PASSWORD = "connection.keyStorePassword";
	public static final String CONN_SERVERURI = "connection.serverURI";
	public static final int DEFAULT_RECONNECTION_BOUND = 5;
	public static final long DEFAULT_RECONNECTION_PERIOD = 60000;
	public static final String MQTTSRC_TOPICDESC_QOS = "qos";
	public static final String MQTTSRC_TOPICDESC_TOPICS = "topics";
	public static final String MQTTSRC_TOPICDESC_ACTION = "action";
	
}
