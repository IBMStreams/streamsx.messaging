/*******************************************************************************
 * Copyright (C) 2013, 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.jms;

import java.io.UnsupportedEncodingException;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.Tuple;

/*
 This is the base interface that defines the common functions
 for all the mesaage classes
 */

interface JMSMessageHandler {

	// Used by JMSSink to convert an incoming tuple to JMS MEssage
	// Each inheriting message class should provide their own
	// implementations
	public Message convertTupleToMessage(Tuple tuple, Session session)
			throws JMSException, UnsupportedEncodingException,
			ParserConfigurationException, TransformerException;

	/*
	 * The MessageNotWriteableException is not caught becuase the message is
	 * created by the operator and is never in read-only mode.
	 */

	// Used by JMSSource to convert an incoming JMS Message to a tuple.
	// Each inheriting message class should provide their own
	// implementations

	public MessageAction convertMessageToTuple(Message message,
			OutputTuple tuple) throws JMSException,
			UnsupportedEncodingException;

};
