// This is a generated header.  Any modifications will be lost.
#ifndef NL_MESSAGINGRESOURCE_H
#define NL_MESSAGINGRESOURCE_H

#include <SPL/Runtime/Utility/FormattableMessage.h>

#define INITIALCONTEXT_NON_EXISTENT \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1400E", "A value for the InitialContext was not specified."))

#define CONFAC_LOOKUP_FAILURE \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1401E", "The system cannot look up the Connection Factory."))

#define DEST_LOOKUP_FAILURE \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1402E", "The system cannot look up the Destination."))

#define CONFAC_DEST_LOOKUP_FAILURE \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1403E", "The system cannot look up the Connection Factory or the Destination."))

#define INITIALCONTEXT_CREATION_ERROR(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1404E", "The InitialContext cannot be created. The exception is: {0}. ", p0))

#define ADMINISTERED_OBJECT_ERROR \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1405E", "Exception occurred during the creation of the administered objects."))

#define CONNECTION_FAILURE_NORETRY \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1406E", "The connection to the IBM Message Service Client failed. The NoRetry value of the reConnection Policy mandates that a reconnection is not allowed."))

#define SET_DELIVERY_MODE_FAILURE(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1407E", "The delivery mode cannot be set. The exception is: {0}.", p0))

#define CREATE_PRODUCER_EXCEPTION(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1408E", "The Producer cannot be created. The exception is: {0}.", p0))

#define CREATE_CONSUMER_EXCEPTION(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1409E", "The Consumer cannot be created. The exception is: {0}.", p0))

#define CREATE_DESTINATION_EXCEPTION(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1410E", "The Destination cannot be created. The exception is: {0}.", p0))

#define CREATE_SESSION_EXCEPTION(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1411E", "The Session cannot be created. The exception is: {0}.", p0))

#define XMS_API_OBJECT_ERROR(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1412E", "An exception occurred during the creation of the IBM Message Service Client Application Programming Interface Objects. The exception is: {0}.", p0))

#define XMS_API_UNKNOWN_EXCEPTION \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1413E", "An unknown exception occurred during the creation of the IBM Message Service Client Application Programming Interface Objects."))

#define CREATE_CONNECTION_EXCEPTION(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1414E", "The Connection cannot be created. The exception is: {0}.", p0))

#define XMS_JMS_EXCEPTION(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1415E", "{0}.", p0))

#define PREVIOUS_ERROR \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1416E", "The system cannot proceed because of the previous errors."))

#define MESSAGE_DROPPED \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1417E", "The message was dropped because the system was not connected to the IBM Message Service Client."))

#define EXCEPTION(p0, p1) \
   (::SPL::FormattableMessage2<typeof(p0),typeof(p1)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1418E", "An exception occurred while the system was sending the message. The exception is: {0}. The error code is: {1}.", p0, p1))

#define STREAMS_EXCEPTION(p0, p1) \
   (::SPL::FormattableMessage2<typeof(p0),typeof(p1)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1419E", "An InfoSphere Streams exception occurred while the system was sending the message. The exception is: {0}. The explanation is: {1}.", p0, p1))

#define OTHER_EXCEPTION(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1420E", "Other exception occurred while the system was sending the message. The exception is: {0}.", p0))

#define UNKNOWN_EXCEPTION \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1421E", "An unknown exception occurred while the system was sending the message."))

#define MESSAGE_LISTENER_ERROR(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1422E", "The message listener with ID cannot be started. The exception is: {0}.", p0))

#define UNABLE_PROCESS_MESSAGE(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1423E", "The incoming message cannot be processed. The exception is: {0}. ", p0))

#define TXT_UNSUPPORTED(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1424E", "Text messages are not supported. The access specification cannot be used: {0}.", p0))

#define OBJ_UNSUPPORTED(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1425E", "Object messages are not supported. The access specification cannot be used: {0}.", p0))

#define PROTOCOLTYPE_NON_SSL1 \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1444E", "Please set the protocol type to ssl when trustStore is provided."))

#define TRUSTSTORE_NON_EXISTENT \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1445E", "Trust Store does not exist."))

#define PROTOCOLTYPE_NON_SSL2 \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1446E", "Please set the protocol type to ssl when keyStore is provided."))

#define KEYSTORE_NON_EXISTENT \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1447E", "Key Store does not exist."))

#define KS_TS_ABSENT_SSL \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1448E", "KeyStore or trustStore should be provided when ssl protocol is used."))

#define BAD_RETURN_CODE(p0, p1) \
   (::SPL::FormattableMessage2<typeof(p0),typeof(p1)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1449E", "Bad return code from MQTTClient_publishMessage returncode={0} handlecode={1}.", p0, p1))

#define RETRY_PUBLISH(p0, p1) \
   (::SPL::FormattableMessage2<typeof(p0),typeof(p1)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1450E", "Error occurred while trying to publish message - the return code is {0} and handle code is {1}, retrying to publish.", p0, p1))

#define CLNT_CREATE_FAILED_NO_RECONNECT \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1451E", "Creation of MQTT client failed. Did not try to reconnect as the policy is noRetry."))

#define CLNT_CREATE_FAILED_BOUNDED_RETRY(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1452E", "Creation of MQTT client failed after trying for {0} times. Did not try to reconnect as the policy is BoundedRetry.", p0))

#define CONN_FAILURE_NO_RECONNECT \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1453E", "Connection to MQTT failed. Did not try to reconnect as the policy is NoRetry. "))

#define CONN_FAILURE_BOUNDED_RETRY(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1454E", "Connection to MQTT failed after retrying for {0} times. Did not try to reconnect as the policy is BoundedRetry.", p0))

#define CONTEXT_NULL \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1455E", "Context information is null."))

#define CONN_ESTABLISH_FAILURE \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1456E", "Failed to establish connection, exiting."))

#define SUBSCRIBE_FAILURE \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1457E", "Failed to subscribe, exiting."))

#define DISCONNECT_START_FAILED(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1458E", "Failed to start disconnect, return code: {0}", p0))

#define SUBSCRIBE_FAILED \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1459E", "Subscribe failed, will abort the application."))

#define INVALID_QOS \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1500E", "Invalid QoS value provided, supported values are 0,1,2."))

#define TOPIC_QOS_SIZE \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1501E", "QoS value should be provided for every topic. "))

#define TRUST_SSL \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1502E", "Set the protocol type to ssl when trustStore is provided."))

#define TRUST_STORE_MISSING \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1503E", "TrustStore file could not be loaded."))

#define KEY_SSL \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1504E", "Set the protocol type to ssl when keyStore is provided."))

#define KEY_STORE_MISSING \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1505E", "KeyStore file could not be loaded."))

#define STORE_SSL \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1506E", "KeyStore or TrustStore should be provided when ssl protocol is being used."))

#define CONNECT_MQTT_FAIL_ABORT \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1507E", "Connection to MQTT failed, Aborting."))

#define BAD_RETRY_PUBLISH(p0, p1) \
   (::SPL::FormattableMessage2<typeof(p0),typeof(p1)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1508E", "An error occurred that can not be handled while trying to publish message, with return code {0}, handle code is {1}, exiting.", p0, p1))

#define PERIOD_NON_NEGATIVE \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1509E", "Period value cannot be negative."))

#define DISCARD_MSG_WRONG_TYPE(p0, p1) \
   (::SPL::FormattableMessage2<typeof(p0),typeof(p1)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1460W", "The message has the wrong type and is being discarded. The expected message type was {0}, but the a {1} message was received.", p0, p1))

#define DISCARD_MSG_INVALID_LENGTH(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1461W", "The message was discarded because the message length of {0} is not valid.", p0))

#define DISCARD_MSG_TOO_SHORT \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1462W", "The message was discarded because the message is too short."))

#define DISCARD_MSG_MISSING_BODY(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1463W", "The message was discarded because the message does not contain a body. A {0} message was expected.", p0))

#define DISCARD_MSG_UNRECOG_TYPE \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1464W", "The message was discarded because the type is not recognized."))

#define DISCARD_MSG_MISSING_ATTR \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1465W", "The message was discarded because the at least one attribute is missing."))

#define INVALID_QOS_WARN \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1466W", "Invalid QoS value provided, supported value is 0,1 or 2. Hence,default value of zero will be used."))

#define TOPIC_QOS_SIZE_WARN \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1467W", "QoS value should be provided for every topic. Hence, control signal will be ignored."))

#define STORE_SSL_WARN \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1468W", "KeyStore or TrustStore should be provided when ssl protocol is being used. Hence, supplied configuration cannot be applied."))

#define KEY_SSL_WARN \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1469W", "Set the protocol type to ssl when keyStore is provided. Hence, supplied configuration is invalid."))

#define TRUST_SSL_WARN \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1470W", "Set the protocol type to ssl when trustStore is provided."))

#define SERVER_URI_WARN \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1471W", "MQTTConfig control tuple does not contain new serverURI. Hence, the configuration will be ignored."))

#define REMOVE_INVALID \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1472W", "Atleast one topic should be provided, cannot remove all. Hence, the configuration will be ignored."))

#define INVALID_MQTT_CONFIG_VALUE \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1473W", "Invalid configuration key provided for mqttConfig attribute. Supported values are connection.serverURI, connection.keyStore, connection.keyStorePassword , connection.trustStore. Hence control signal will be ignored."))

#define XMS_CONNECT \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1520I", "The system is about to connect to the IBM Message Service Client."))

#define CONNECTION_FAILURE_BOUNDEDRETRY(p0, p1) \
   (::SPL::FormattableMessage2<typeof(p0),typeof(p1)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1521I", "The connection to the IBM Message Service Client failed. The system is waiting for {0} seconds before it reconnects. The number of connection attempts is: {1}.", p0, p1))

#define CONNECTION_FAILURE_INFINITERETRY(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1522I", "The connection to the IBM Message Service Client failed. The system is waiting for {0} seconds before it reconnects.", p0))

#define CONNECTION_SUCCESSFUL \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1523I", "The connection to the IBM Message Service Client succeeded."))

#define SEND_TUPLE_ERROR_PORT \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1524I", "The tuple is being sent to the error output port."))

#define SENT_MESSAGE(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1525I", "The message was sent. The ID is: {0}.", p0))

#define INIT_WAIT(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1526I", "The wait time before starting is {0} seconds.", p0))

#define MQTT_CLIENT_DISCONNECTED \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1528I", "MQTT Client Disconnected."))

#define PUBLISH_SUCCESS(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1529I", "Successfully published message for client {0}.", p0))

#define RECONNECT_ATTEMPT(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1530I", "Attempting to connect with clientId {0}.", p0))

#define MQTT_CLNT_CREATION_FAILED(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1531I", "Failed to create MQTT Client instance, with return code {0} , retrying.", p0))

#define RETRY_CONNECT \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1532I", "Connection to MQTT failed, Trying to reconnect."))

#define CONN_NOT_SUCCESS \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1533I", "Connection is unsuccessfull. "))

#define CONN_SUCCESS \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1534I", "Connection is successful."))

#define CONN_ESTABLISH_FAIL \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1535I", "Failed to establish connection."))

#define MSG_RECEIVED(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1536I", "Message received on topic {0} .", p0))

#define SUBSCRIBE_SUCCESS \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1537I", "Subscribe succeeded, waiting to receive messages."))

#define INTERMITTENT_FAIL \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1538I", "Connection failed in between, will try to re-connect."))

#define INVALID_QOS_INFO_TUPLE \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1539I", "Invalid QoS value provided in the incoming tuple, supported value is 0,1 or 2. Hence, default value of Qos=0 is used."))

#endif  // NL_MESSAGINGRESOURCE_H
