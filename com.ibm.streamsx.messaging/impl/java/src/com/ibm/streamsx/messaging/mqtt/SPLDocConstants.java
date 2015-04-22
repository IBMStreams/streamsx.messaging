/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.mqtt;

public class SPLDocConstants {

	// SPL Documentation for MQTTSource
	static final String MQTTSRC_OP_DESCRIPTION = "The MQTTSource operator subscribes to MQTT topics and receives messages when they are published to subscribed topics. You specify the list of topics that you want to subscribe to and the quality of service (QoS) for the topics when you connect to the MQTT server. You can update the list of topics and the QoS levels at run time by using the optional control input port.\\n\\n"
			+ "**Behavior in a consistent region** \\n\\n"
			+ "MQTTSource operator cannot be part of a consistent region. Connect MQTTSource operator to an ReplayableStart operator from the Standard Toolkit to achieve tuple replay. "
			+ "Messages are stored in an internal buffer before they are submitted to the output port.  To reduce the chance of tuple loss during application failure, you may use the \'messageQueueSize\' parameter to control this internal buffer size.";
	static final String MQTTSRC_OUTPUT_PORT_1 = "The optional output port is an error port where the operator submits a single tuple for each failed message. The tuple contains a single attribute of type rstring or ustring, which contains the details of the error message.";
	static final String MQTTSRC_OUPUT_PORT_0 = "This is the data port and is mandatory.";
	static final String MQTTSRC_INPUT_PORT0 = "This is the optional control port.  You can use the control port to update information at run time, such as the connection information that the operator uses to connect to an MQTT server, the topics that the operator subscribes to, or the QoS levels of the subscribed topics.";
	static final String MQTTSRC_PARAM_PERIOD_DESC = "This parameter specifies the time period in seconds the operator waits before it tries to reconnect. It is an optional parameter of type float64. Default value is 60000 ms.";
	static final String MQTTSRC_PARAM_RECONN_BOUND_DESC = "This optional parameter of type int32 specifies the number of successive connections that are attempted for an operator.  Specify 0 for no retry, n for n number of retries, -1 for inifinite retry.";
	static final String MQTTSRC_PARAM_TOPICATTRNAME_DESC = "Output attribute on output data stream to assign message topic to.";
	static final String MQTTSRC_PARAM_ERRORATTRNAME_DESC = "Output attribute on optional error output port to assign error message to.";
	static final String MQTTSRC_PARAM_SERVERIURI_DESC = "This optional parameter of type rstring specifies the URI of the MQTT server to connect to. The serverURI has the following format:  "
			+ "protocol://hostname or IP address:portnumber. "
			+ "The supported protocols are SSL and TCP. To use SSL authentication, set the protocol to ssl.";
	static final String MQTTSRC_PARAM_QOS_DESC = "List of qos for topic subscriptions, this attribute is mutually exclusive with qosStr attribute.";
	static final String MQTTSRC_PARAM_QOS_STR_DESC = "List of qos in string format for topic subscriptions. Multiple comma separated qos value can be specified, for example \\\"0, 1\\\". This attribute is mutually exclusive with qos attribute.";
	static final String MQTTSRC_PARAM_TOPICS_DESC = "List of topics to subscribe to. Multiple comma separated topics can be specified, for example \\\"topic1, topic2\\\"";
	static final String MQTTSRC_PARAM_MESSAGE_SIZE_DESC = "Specify size of internal buffer for queueing incoming tuples. The default buffer size is 50 tuples.";

	// SPL Documnetation for MQTTSink
	static final String MQTTSINK_OP_DESCRIPTION = "The MQTTSink operator creates a message for every tuple it receives on its input port and publishes the message to an MQTT server. You can use the topic parameter to specify the topic that you want to publish to, or you can use an attribute from the input tuple to identify the topic that you want to publish to at run time.  The input tuple can optionally contain the topic and QoS for the message. This information is not considered to be part of the data.  \\n\\n"
			+ "**Behavior in a consistent region** \\n\\n"
			+ "MQTTSink operator can be an operator within the reachability graph of a consistent region, but it can not be placed at start of a consistent region. "
			+ "Having a control port in a consistent region is not supported.  The control information may not be replayed, persisted and restored correctly. "
			+ "You may need to manually replay the control signals to bring the operator back to a consistent state.\\n\\n"
			+ "When the  MQTTSink operator is in a consistent region, messages with qos=1 or qos=2 will be delivered to an MQTT provider at least once.  Messages with qos=0 can still be lost as a result of application failures.";

	static final String MQTTSINK_PARAM_QOS_ATTR_NAME_DESC = "Attribute name that contains the qos to publish the message with.  This parameter is mutually exclusive with the \\\"qos\\\" parameter.";

	static final String MQTTSINK_PARAM_TOPIC_ATTR_NAME_DESC = "Attribute name that contains the topic to publish the message with.  This parameter is mutually exclusive with the \\\"topic\\\" parameter.";

	static final String MQTTSINK_PARAM_RETAIN_DESC = "Indicates if messages should be retained on the MQTT server.  Default is false.";

	static final String MQTTSINK_PARAM_PERIOD_DESC = "This parameter specifies the time period in seconds the operator waits before it tries to reconnect. It is an optional parameter of type float64. Default value is 60000 ms.";

	static final String MQTTSINK_PARAM_RECONN_BOUND_DESC = "This optional parameter of type int32 specifies the number of successive connections that are attempted for an operator.  Specify 0 for no retry, n for n number of retries, -1 for inifinite retry.";

	static final String MQTTSINK_PARAM_SERVERURI_DESC = "This optional parameter of type rstring specifies the URI of the MQTT server to connect to. The serverURI has the following format:  "
			+ "protocol://hostname or IP address:portnumber. "
			+ "The supported protocols are SSL and TCP. To use SSL authentication, set the protocol to ssl.";

	static final String MQTTSINK_PARAM_QOS_DESC = "This optional parameter of type int32 specifies the quality of service that the MQTTSink operator provides for each MQTT message that it publishes to an MQTT topic. The valid values are 0, 1, and 2. The default value is 0.\\n\\n"
			+ "Important: The quality of service is provided by the MQTT server to its subscribers. For each message that it publishes, the MQTTSink operator passes the value of the qos parameter as a part of the message header to the MQTT server.\\n\\n"
			+ "If the qos parameter is set to 0, there is no guarantee that the message is received by the MQTT server or is handled by any of the message subscribers. The operator publishes the message at most once. No further attempts are made to publish the message again, and the message is lost in case of failures. If the qos value is set to 1 or 2, the operator publishes the message and waits until it receives an acknowledgment from the MQTT server before it discards the message. However, if the MQTTSink operator terminates unexpectedly while it is processing the input tuple or creating a message, or if there is a connection failure, the message is lost. There is no guarantee that the message is received by the MQTT server. If the MQTTSink operator publishes the topic successfully, the MQTT server ensures that the quality of service that is defined by the qos parameter is provided to the message subscribers.";

	static final String MQTTSINK_PARAM_TOPIC_DESC = "This mandatory parameter of type rstring specifies the MQTT topic that you want to publish to. You can specify a static string, such as \\\"traffic/freeway/880\\\".  This parameter is mutually exclusive with the \\\"topicAttributeName\\\" parameter.";

	public static final String MQTTSINK_OUTPUT_PORT0 = " This port is an error port where a single tuple is sent for each failed message. The tuple contains a single attribute of type rstring, which contains the details of the error message.";

	public static final String MQTTSINK_INPUTPORT1 = "Input port 1 is an optional control port that can be used to update the configuration of the operator at run time.";

	public static final String MQTTSINK_INPUTPORT0 = "Input port 0 is a data port and is mandatory.";
	public static final String PARAM_CONNECTION_DESC = "Name of the connection specification of the MQTT element in the connection document.";
	public static final String PARAM_CONNDOC_DESC = "Path to connection document.  If unspecified, default to applicationDir/etc/connections.xml.  If a relative path is specified, the path is relative to the application directory.";
	public static final String PARAM_TRUSTORE_PW_DESC = "This optional parameter of type rstring specifies the password to decrypt the encrypted trustStore file.";
	public static final String PARAM_KEYSTORE_PW_DESC = "This optional parameter of type rstring specifies the password to decrypt the encrypted keyStore file.";
	public static final String PARAM_KEYSTORE_DESC = "This optional parameter of type rstring specifies the file that contains the public and private key certificates of the MQTT client. If a relative path is specified, the path is relative to the application directory.";
	public static final String PARAM_TRUSTORE_DESC = "This optional parameter of type rstring specifies the name of the file that contains the public certificate of the trusted MQTT server.  If a relative path is specified, the path is relative to the application directory.";

	// Common SPL Documnetation
	public static final String MQTT_PARAM_CLIENT_ID_DESC = "All clients connected to the same server must have a unique ID. This optional parameter allows user to specify a client id to use when connecting to a MQTT provider.  An ID will be generated by the operator if this parameter is not specified.";
	public static final String MQTT_PARAM_USER_ID_DESC = "This optional parameter sets the user name to use for the connection. Must be specified when password parameter is used, or compile time error will occur";
	public static final String MQTT_PARAM_PASSWORD_DESC = "This optional parameter sets the password to use for the connection. Must be specified when userID parameter is used, or compile time error will occur";
	public static final String MQTT_PARAM_COMMAND_TIMEOUT_DESC = "This optional parameter is used to specify maximum time in millisecond to wait for an MQTT action to complete.  A MQTT action can include connecting to a server, or publshing to a message.  A value of 0 will cause the operator to wait indefinitely for an  action to complete.  A negative number will cause a runtime error. If unspecified, the default value for this parameter is 0.";
	public static final String MQTT_PARAM_KEEP_ALIVE_INTERVAL_DESC = "This optional parameter, measured in seconds, sets the maximum time interval between messages sent or received. It enables the client to detect if the server is no longer available. By default, it is set to 60 seconds. A value of 0 will disable it. Negative number will cause a runtime error.";
	public static final String MQTT_PARAM_DATA_ATTRIBUTE_DESC = "This optional parameter specifies the name of the attribute that is used to hold actual content of message, if not specified, in the case where multiple attributes are defined for the streams schema, the operator will look for attribute named data and use it as data attribute. In the case where the schema contains only a signle attribute, the operator will assume that the attribute is the data attribute";
}
