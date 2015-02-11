/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.mqtt;

public class SPLDocConstants {

	// SPL Documentation for MQTTSource
	static final String MQTTSRC_OP_DESCRIPTION = "Java Operator MqttSourceOperator. \\n\\n" 
			                                   + "**Behavior in a consistent region** \\n\\n"
			                                   + "MQTTSource operator does not support consistent region. Connect MQTTSource operator to an ReplayableStart operator to achieve tuple reply, "
			                                   + "it is recomended to set messageQueueSize attribute of MQTTSource operator to 1 when it is connected ReplayableStart operator.";
	static final String MQTTSRC_OUTPUT_PORT_1 = "Optional error output ports.  The error output port is expected to have a single attribute of rstring or ustring.";
	static final String MQTTSRC_OUPUT_PORT_0 = "Port that produces tuples.";
	static final String MQTTSRC_INPUT_PORT0 = "Optional input ports";
	static final String MQTTSRC_PARAM_PERIOD_DESC = "Reconnection period in ms, default is 60000 ms.";
	static final String MQTTSRC_PARAM_RECONN_BOUND_DESC = "Reconnection bound, 0 for no retry, n for n number of retries, -1 for inifinite retry.";
	static final String MQTTSRC_PARAM_TOPICATTRNAME_DESC = "Output attribute on output data stream to assign message topic to.";
	static final String MQTTSRC_PARAM_ERRORATTRNAME_DESC = "Output attribute on optional error output port to assign error message to.";
	static final String MQTTSRC_PARAM_SERVERIURI_DESC = "The URI of the server to connect to";
	static final String MQTTSRC_PARAM_QOS_DESC = "List of qos for topic subscriptions, this attribute is mutually exclusive with qosStr attribute.";
	static final String MQTTSRC_PARAM_QOS_STR_DESC = "List of qos in string format for topic subscriptions. Multiple comma separated qos value can be specified, for example \\\"0, 1\\\". This attribute is mutually exclusive with qos attribute.";
	static final String MQTTSRC_PARAM_TOPICS_DESC = "List of topics to subscribe to. Multiple comma separated topics can be specified, for example \\\"topic1, topic2\\\"";
	static final String MQTTSRC_PARAM_MESSAGE_SIZE_DESC = "Specify size of internal buffer for queueing incoming tuples. By default, internal buffer can hold up to 50 tuples.";

	// SPL Documnetation for MQTTSink
	static final String MQTTSINK_OP_DESCRIPTION = "MQTTSink operator publishes messages to MQTT Provider. \\n\\n"
			                                    + "**Behavior in a consistent region** \\n\\n"
			                                    + "MQTTSink operator can be an operator within the reachability graph of a consistent region, but it can not be placed at start of a consistent region. "
			                                    + "Having a control port in a consistent region is not supported.  The control information may not be replayed, persisted and restored correctly. "
			                                    + "You may need to manually replay the control signals to bring the operator back to a consistent state.";

	static final String MQTTSINK_PARAM_QOS_ATTR_NAME_DESC = "Attribute name that contains the qos to publish the message with.  This parameter is mutually exclusive with the \\\"qos\\\" parameter.";

	static final String MQTTSINK_PARAM_TOPIC_ATTR_NAME_DESC = "Attribute name that contains the topic to publish the message with.  This parameter is mutually exclusive with the \\\"topic\\\" parameter.";

	static final String MQTTSINK_PARAM_RETAIN_DESC = "Indicates if messages should be retained on the MQTT server.  Default is false.";

	static final String MQTTSINK_PARAM_PERIOD_DESC = "Reconnection period in ms, default is 60000 ms.";

	static final String MQTTSINK_PARAM_RECONN_BOUND_DESC = "Reconnection bound, 0 for no retry, n for n number of retries, -1 for inifinite retry.";

	static final String MQTTSINK_PARAM_SERVERURI_DESC = "The URI of the server to connect to.";

	static final String MQTTSINK_PARAM_QOS_DESC = "Qos to publish to.";

	static final String MQTTSINK_PARAM_TOPIC_DESC = "Topic to publish to.  This parameter is mutually exclusive with the \\\"topicAttributeName\\\" parameter.";

	public static final String MQTTSINK_OUTPUT_PORT0 = "Optional error output port.";

	public static final String MQTTSINK_INPUTPORT1 = "Optional input ports";

	public static final String MQTTSINK_INPUTPORT0 = "Port that ingests tuples.";
	public static final String PARAM_CONNECTION_DESC = "Name of the connection specification of the MQTT element in the connection document.";
	public static final String PARAM_CONNDOC_DESC = "Path to connection document.  If unspecified, default to applicationDir/etc/connections.xml.  If a relative path is specified, the path is relative to the application directory.";
	public static final String PARAM_TRUSTORE_PW_DESC = "This optional parameter of type rstring specifies the truststore password.";
	public static final String PARAM_KEYSTORE_PW_DESC = "This optional parameter of type rstring specifies keystore password.";
	public static final String PARAM_KEYSTORE_DESC = "This optional parameter of type rstring specifies the file that contains the public and private key certificates of the MQTT client.  If a relative path is specified, the path is relative to the application directory.";
	public static final String PARAM_TRUSTORE_DESC = "The parameter of type rstring specifies the name of the file that contains the public certificate of the trusted MQTT server.  If a relative path is specified, the path is relative to the application directory.";

	// Common SPL Documnetation
	public static final String MQTT_PARAM_CLIENT_ID_DESC = "All clients connected to the same server must have a unique ID. This optional parameter allows user to specify a client id or the operator will generate one for you";
    public static final String MQTT_PARAM_USER_ID_DESC = "This optional parameter sets the user name to use for the connection. Must be specified when password parameter is used, or compile time error will occur";
    public static final String MQTT_PARAM_PASSWORD_DESC = "This optional parameter sets the password to use for the connection. Must be specified when userID parameter is used, or compile time error will occur";
    public static final String MQTT_PARAM_COMMAND_TIMEOUT_DESC = "This optional parameter is used to specify maximum time in millisecond to wait for an MQTT action to complete instead of waiting until a specific action to finish such as message publish action. A value of 0 will wait until the action finishes and not timeout, negative number will cause a runtime error. By default, the operator will not timeout";
    public static final String MQTT_PARAM_KEEP_ALIVE_INTERVAL_DESC = "This optional parameter, measured in seconds, sets the maximum time interval between messages sent or received. It enables the client to detect if the server is no longer available. By default, it is set to 60 seconds. A value of 0 will disable it. Negative number will cause a runtime error.";
    public static final String MQTT_PARAM_DATA_ATTRIBUTE_DESC = "This optional parameter specifies the name of the attribute that is used to hold actual content of message, if not specified, default data attribute name is data";
}
