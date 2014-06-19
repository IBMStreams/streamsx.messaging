/* begin_generated_IBM_copyright_prolog                             */
/*                                                                  */
/* This is an automatically generated copyright prolog.             */
/* After initializing,  DO NOT MODIFY OR MOVE                       */
/* **************************************************************** */
/* IBM Confidential                                                 */
/* OCO Source Materials                                             */
/* 5724-Y95                                                         */
/* (C) Copyright IBM Corp.  2013, 2013                              */
/* The source code for this program is not published or otherwise   */
/* divested of its trade secrets, irrespective of what has          */
/* been deposited with the U.S. Copyright Office.                   */
/*                                                                  */
/* end_generated_IBM_copyright_prolog                               */
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
