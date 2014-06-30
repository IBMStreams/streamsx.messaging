/*******************************************************************************
 * Copyright (C) 2013, 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.jms;

//enum which defines the action which is returned
//from the function convertMessageToTuple function for JMSSource

//DISCARD_MESSAGE_WRONG_TYPE=The incoming message type does not
//match the expected message type
//DISCARD_MESSAGE_EOF_REACHED= When for the BytesMessage or StreamsMessage
//if unexpected end of message has been reached
//DISCARD_MESSAGE_UNREADABLE=if the message is in write-only mode
//SUCCESSFUL_MESSAGE=if the JMS Message was read succesfully
//DISCARD_MESSAGE_MESSAGE_FORMAT_ERROR= if the type conversion is invalid.

enum MessageAction {
	DISCARD_MESSAGE_WRONG_TYPE, DISCARD_MESSAGE_EOF_REACHED, DISCARD_MESSAGE_UNREADABLE, SUCCESSFUL_MESSAGE, DISCARD_MESSAGE_MESSAGE_FORMAT_ERROR
}
