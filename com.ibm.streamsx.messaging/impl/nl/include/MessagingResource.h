// This is a generated header.  Any modifications will be lost.
#ifndef NL_MESSAGINGRESOURCE_H
#define NL_MESSAGINGRESOURCE_H

#include <SPL/Runtime/Utility/FormattableMessage.h>

#define MSGTK_INITIALCONTEXT_NON_EXISTENT \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1400E", "A value for the InitialContext was not specified."))

#define MSGTK_CONFAC_LOOKUP_FAILURE \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1401E", "The system cannot look up the Connection Factory."))

#define MSGTK_DEST_LOOKUP_FAILURE \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1402E", "The system cannot look up the Destination."))

#define MSGTK_CONFAC_DEST_LOOKUP_FAILURE \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1403E", "The system cannot look up the Connection Factory or the Destination."))

#define MSGTK_INITIALCONTEXT_CREATION_ERROR(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1404E", "The InitialContext cannot be created. The exception is: {0}. ", p0))

#define MSGTK_ADMINISTERED_OBJECT_ERROR \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1405E", "Exception occurred during the creation of the administered objects."))

#define MSGTK_CONNECTION_FAILURE_NORETRY \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1406E", "The connection to the IBM Message Service Client failed. The NoRetry value of the reConnection Policy mandates that a reconnection is not allowed."))

#define MSGTK_SET_DELIVERY_MODE_FAILURE(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1407E", "The delivery mode cannot be set. The exception is: {0}.", p0))

#define MSGTK_CREATE_PRODUCER_EXCEPTION(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1408E", "The Producer cannot be created. The exception is: {0}.", p0))

#define MSGTK_CREATE_CONSUMER_EXCEPTION(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1409E", "The Consumer cannot be created. The exception is: {0}.", p0))

#define MSGTK_CREATE_DESTINATION_EXCEPTION(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1410E", "The Destination cannot be created. The exception is: {0}.", p0))

#define MSGTK_CREATE_SESSION_EXCEPTION(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1411E", "The Session cannot be created. The exception is: {0}.", p0))

#define MSGTK_XMS_API_OBJECT_ERROR(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1412E", "An exception occurred during the creation of the IBM Message Service Client Application Programming Interface Objects. The exception is: {0}.", p0))

#define MSGTK_XMS_API_UNKNOWN_EXCEPTION \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1413E", "An unknown exception occurred during the creation of the IBM Message Service Client Application Programming Interface Objects."))

#define MSGTK_CREATE_CONNECTION_EXCEPTION(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1414E", "The Connection cannot be created. The exception is: {0}.", p0))

#define MSGTK_XMS_JMS_EXCEPTION(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1415E", "{0}.", p0))

#define MSGTK_PREVIOUS_ERROR \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1416E", "The system cannot proceed because of the previous errors."))

#define MSGTK_MESSAGE_DROPPED \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1417E", "The message was dropped because the system was not connected to the IBM Message Service Client."))

#define MSGTK_EXCEPTION(p0, p1) \
   (::SPL::FormattableMessage2<typeof(p0),typeof(p1)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1418E", "An exception occurred while the system was sending the message. The exception is: {0}. The error code is: {1}.", p0, p1))

#define MSGTK_STREAMS_EXCEPTION(p0, p1) \
   (::SPL::FormattableMessage2<typeof(p0),typeof(p1)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1419E", "An InfoSphere Streams exception occurred while the system was sending the message. The exception is: {0}. The explanation is: {1}.", p0, p1))

#define MSGTK_OTHER_EXCEPTION(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1420E", "Other exception occurred while the system was sending the message. The exception is: {0}.", p0))

#define MSGTK_UNKNOWN_EXCEPTION \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1421E", "An unknown exception occurred while the system was sending the message."))

#define MSGTK_MESSAGE_LISTENER_ERROR(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1422E", "The message listener with ID cannot be started. The exception is: {0}.", p0))

#define MSGTK_UNABLE_PROCESS_MESSAGE(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1423E", "The incoming message cannot be processed. The exception is: {0}. ", p0))

#define MSGTK_TXT_UNSUPPORTED(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1424E", "Text messages are not supported. The access specification cannot be used: {0}.", p0))

#define MSGTK_OBJ_UNSUPPORTED(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1425E", "Object messages are not supported. The access specification cannot be used: {0}.", p0))

#define MSGTK_DISCARD_MSG_WRONG_TYPE(p0, p1) \
   (::SPL::FormattableMessage2<typeof(p0),typeof(p1)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1460W", "The message has the wrong type and is being discarded. The expected message type was {0}, but the a {1} message was received.", p0, p1))

#define MSGTK_DISCARD_MSG_INVALID_LENGTH(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1461W", "The message was discarded because the message length of {0} is not valid.", p0))

#define MSGTK_DISCARD_MSG_TOO_SHORT \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1462W", "The message was discarded because the message is too short."))

#define MSGTK_DISCARD_MSG_MISSING_BODY(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1463W", "The message was discarded because the message does not contain a body. A {0} message was expected.", p0))

#define MSGTK_DISCARD_MSG_UNRECOG_TYPE \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1464W", "The message was discarded because the type is not recognized."))

#define MSGTK_DISCARD_MSG_MISSING_ATTR \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1465W", "The message was discarded because the at least one attribute is missing."))

#define MSGTK_XMS_CONNECT \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1520I", "The system is about to connect to the IBM Message Service Client."))

#define MSGTK_CONNECTION_FAILURE_BOUNDEDRETRY(p0, p1) \
   (::SPL::FormattableMessage2<typeof(p0),typeof(p1)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1521I", "The connection to the IBM Message Service Client failed. The system is waiting for {0} seconds before it reconnects. The number of connection attempts is: {1}.", p0, p1))

#define MSGTK_CONNECTION_FAILURE_INFINITERETRY(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1522I", "The connection to the IBM Message Service Client failed. The system is waiting for {0} seconds before it reconnects.", p0))

#define MSGTK_CONNECTION_SUCCESSFUL \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1523I", "The connection to the IBM Message Service Client succeeded."))

#define MSGTK_SEND_TUPLE_ERROR_PORT \
   (::SPL::FormattableMessage0("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1524I", "The tuple is being sent to the error output port."))

#define MSGTK_SENT_MESSAGE(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1525I", "The message was sent. The ID is: {0}.", p0))

#define MSGTK_INIT_WAIT(p0) \
   (::SPL::FormattableMessage1<typeof(p0)>("com.ibm.streamsx.messaging", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1526I", "The wait time before starting is {0} seconds.", p0))

#endif  // NL_MESSAGINGRESOURCE_H
