/*******************************************************************************
 * Copyright (C) 2016, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.messaging.common;

public interface IGovernanceConstants {
	public static final String TAG_OPERATOR_IGC = "OperatorIGC"; //$NON-NLS-1$
	public static final String TAG_REGISTER_TYPE = "registerType"; //$NON-NLS-1$
	public static final String TAG_REGISTER_TYPE_INPUT = "input"; //$NON-NLS-1$
	public static final String TAG_REGISTER_TYPE_OUTPUT = "output"; //$NON-NLS-1$
	
	public static final String ASSET_STREAMS_PREFIX = "$Streams-"; //$NON-NLS-1$
	
	public static final String ASSET_JMS_SERVER_TYPE = "JMSServer"; //$NON-NLS-1$
	
	public static final String ASSET_JMS_MESSAGE_TYPE = "JMS"; //$NON-NLS-1$

	public static final String ASSET_KAFKA_TOPIC_TYPE = "KafkaTopic"; //$NON-NLS-1$
	
	public static final String ASSET_MQTT_TOPIC_TYPE = "MQTT"; //$NON-NLS-1$
	public static final String ASSET_MQTT_SERVER_TYPE = "MQServer"; //$NON-NLS-1$
	
	public static final String PROPERTY_SRC_NAME = "srcName"; //$NON-NLS-1$
	public static final String PROPERTY_SRC_TYPE = "srcType"; //$NON-NLS-1$
	
	public static final String PROPERTY_SRC_PARENT_PREFIX = "srcParent"; //$NON-NLS-1$
	public static final String PROPERTY_PARENT_TYPE = "parentType"; //$NON-NLS-1$
	
	public static final String PROPERTY_PARENT_PREFIX = "p1"; //$NON-NLS-1$
	
	public static final String PROPERTY_INPUT_OPERATOR_TYPE = "inputOperatorType"; //$NON-NLS-1$
	public static final String PROPERTY_OUTPUT_OPERATOR_TYPE = "outputOperatorType"; //$NON-NLS-1$

}
