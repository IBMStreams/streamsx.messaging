/*******************************************************************************
 * Copyright (C) 2016, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.messaging.common;

public interface IGovernanceConstants {
	public static final String TAG_OPERATOR_IGC = "OperatorIGC";
	public static final String TAG_REGISTER_TYPE = "registerType";
	public static final String TAG_REGISTER_TYPE_INPUT = "input";
	public static final String TAG_REGISTER_TYPE_OUTPUT = "output";
	
	public static final String ASSET_STREAMS_PREFIX = "$Streams-";
	
	public static final String ASSET_JMS_SERVER_TYPE = "JMSServer";
	
	public static final String ASSET_JMS_MESSAGE_TYPE = "JMS";

	public static final String ASSET_KAFKA_TOPIC_TYPE = "KafkaTopic";
	
	public static final String ASSET_MQTT_TOPIC_TYPE = "MQTT";
	public static final String ASSET_MQTT_SERVER_TYPE = "MQServer";
	
	public static final String PROPERTY_SRC_NAME = "srcName";
	public static final String PROPERTY_SRC_TYPE = "srcType";
	
	public static final String PROPERTY_SRC_PARENT_PREFIX = "srcParent";
	public static final String PROPERTY_PARENT_TYPE = "parentType";
	
	public static final String PROPERTY_PARENT_PREFIX = "p1";
	
	public static final String PROPERTY_INPUT_OPERATOR_TYPE = "inputOperatorType";
	public static final String PROPERTY_OUTPUT_OPERATOR_TYPE = "outputOperatorType";

}
