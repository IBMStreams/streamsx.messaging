/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.mqtt;

public interface IMqttConstants {

	public static final String CONN_TRUSTSTORE = "connection.trustStore"; //$NON-NLS-1$
	public static final String CONN_KEYSTORE = "connection.keyStore"; //$NON-NLS-1$
	public static final String CONN_KEYSTORE_PASSWORD = "connection.keyStorePassword"; //$NON-NLS-1$
	public static final String CONN_SERVERURI = "connection.serverURI"; //$NON-NLS-1$
	public static final int DEFAULT_RECONNECTION_BOUND = 5;
	public static final long DEFAULT_RECONNECTION_PERIOD = 60000;
	public static final String MQTTSRC_TOPICDESC_QOS = "qos"; //$NON-NLS-1$
	public static final String MQTTSRC_TOPICDESC_TOPICS = "topics"; //$NON-NLS-1$
	public static final String MQTTSRC_TOPICDESC_ACTION = "action"; //$NON-NLS-1$
	public static final int MQTTSRC_DEFAULT_QUEUE_SIZE = 50;
	public static final String SSL_KEY_STORE_PASSWORD = "com.ibm.ssl.keyStorePassword"; //$NON-NLS-1$
	public static final String SSL_KEY_STORE = "com.ibm.ssl.keyStore"; //$NON-NLS-1$
	public static final String SSL_TRUST_STORE = "com.ibm.ssl.trustStore"; //$NON-NLS-1$
	static final String SSK_TRUST_STORE_PASSWORD = "com.ibm.ssl.trustStorePassword"; //$NON-NLS-1$
	public static final long UNINITIALIZED_COMMAND_TIMEOUT = -1L;
	public static final int UNINITIALIZED_KEEP_ALIVE_INTERVAL = -1;
	public static final long CONSISTENT_REGION_DRAIN_WAIT_TIME = 180000;
	public static final String MQTT_DEFAULT_DATA_ATTRIBUTE_NAME = "data"; //$NON-NLS-1$
	public static final String COMMA = ","; //$NON-NLS-1$
	public static final String SSL_PROTOCOL = "com.ibm.ssl.protocol"; //$NON-NLS-1$
	public static final String DEFAULT_SSL_PROTOCOL = "TLSv1.2"; //$NON-NLS-1$
}
