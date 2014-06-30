/*******************************************************************************
 * Copyright (C) 2013, 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.jms;

import java.util.List;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.OutputTuple;

//Empty message class is used in JMSSink/JMSSource to send/receive
// control message to the JMSPorvider to test initial connectivity.

class EmptyMessageHandler extends JMSMessageHandlerImpl {

	// Constructor
	EmptyMessageHandler(List<NativeSchema> nativeSchemaObjects) {
		super(nativeSchemaObjects);
	}

	// Used by JMSSink to convert an incoming tuple to JMS MEssage
	public Message convertTupleToMessage(Tuple tuple, Session session)
			throws JMSException {

		synchronized (session) {
			// simply create a new JMSMEssage and return
			return session.createMessage();
		}

	}

	// Used by JMSSource to convert an incoming JMS Message to a tuple
	public MessageAction convertMessageToTuple(Message message,
			OutputTuple tuple) {
		// No validations are performed regarding the type of message
		// No values are assigned to tuple elements.
		return MessageAction.SUCCESSFUL_MESSAGE;
	}
}
