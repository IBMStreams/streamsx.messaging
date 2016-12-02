/*******************************************************************************
 * Copyright (C) 2013, 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.jms;

import java.util.List;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

//import com.ibm.rmi.corba.ObjectManager; ... not used
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.Type.MetaType;

//Empty message class is used in JMSSink/JMSSource to send/receive
// control message to the JMSPorvider to test initial connectivity.

class EmptyMessageHandler extends JMSMessageHandlerImpl {

	// Constructor
	EmptyMessageHandler(List<NativeSchema> nativeSchemaObjects) {
		super(nativeSchemaObjects);
	}

	// Used by JMSSink to convert an incoming tuple to JMS MEssage
	public Message convertTupleToMessage(Tuple tuple, Session session) throws JMSException {

		synchronized (session) {
			// simply create a new JMSMEssage and return
			return session.createMessage();
		}
	}

	// Used by JMSSource to convert an incoming JMS Message to a tuple
	public MessageAction convertMessageToTuple(Message message, OutputTuple tuple) {

		MetaType attrType = tuple.getStreamSchema().getAttribute(0).getType().getMetaType();

		// if the first attribute is a RString, encode message information into
		// the rstring
		if (attrType == MetaType.RSTRING) {
			try {
				String msgId = message.getJMSMessageID();
				long expiration = message.getJMSExpiration();
				String type = getMessageType(message);
				String deliveryModeStr = getDeliveryMode(message);

				// format is "messagetype, msgid, deliveryMode, expiration"
				StringBuilder builder = new StringBuilder();
				builder.append(type);
				builder.append(","); //$NON-NLS-1$
				builder.append(msgId);
				builder.append(","); //$NON-NLS-1$
				builder.append(deliveryModeStr);
				builder.append(","); //$NON-NLS-1$
				builder.append(expiration);

				tuple.setString(0, builder.toString());
			} catch (JMSException e) {
			}
		}

		return MessageAction.SUCCESSFUL_MESSAGE;
	}

	private String getMessageType(Message message) {
		if (message instanceof BytesMessage)
			return "bytes"; //$NON-NLS-1$
		else if (message instanceof StreamMessage)
			return "streams"; //$NON-NLS-1$
		else if (message instanceof MapMessage)
			return "map"; //$NON-NLS-1$
		else if (message instanceof TextMessage)
			return "text"; //$NON-NLS-1$
		else if (message instanceof ObjectMessage)
			return "object"; //$NON-NLS-1$
		else if (message instanceof Message)
			return "empty"; //$NON-NLS-1$
		return "unknown"; //$NON-NLS-1$
	}

	private String getDeliveryMode(Message message) {
		String deliveryModeStr = "unknown"; //$NON-NLS-1$
		int deliveryMode = -1;
		try {
			deliveryMode = message.getJMSDeliveryMode();
		} catch (JMSException e) {
		}

		if (deliveryMode == DeliveryMode.NON_PERSISTENT) {
			deliveryModeStr = "non_persistent"; //$NON-NLS-1$
		} else if (deliveryMode == DeliveryMode.PERSISTENT) {
			deliveryModeStr = "persistent"; //$NON-NLS-1$
		}
		return deliveryModeStr;
	}
}
