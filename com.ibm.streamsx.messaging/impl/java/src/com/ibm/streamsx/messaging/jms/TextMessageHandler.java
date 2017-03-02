/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.jms;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.types.RString;
import com.ibm.streams.operator.types.ValueFactory;
import com.ibm.streams.operator.types.XML;

/**
 * 
 * Message handler for text message class.
 * 
 * For text message class, the following restrictions applies:
 * * Text messages must be encoded in UTF-8.  For support of other encoding, use bytes message class.
 * * When the text message class is specified in the connection document, the native
 * schema must contain a single attribute of String.  
 * * When the text message class is specified in the connection document, the input schema
 * for JMSSource must contain a single attribute of type rstring, ustring or xml.
 * When the text message class is specified in the connection document, the output schema
 * for JMSSink must contain a single attribute of type rstring, ustring or xml.
 *
 */
public class TextMessageHandler extends JMSMessageHandlerImpl {

	private int length;

	public TextMessageHandler(List<NativeSchema> nsa) {
		super(nsa);

		if (nsa.size() == 1) {
			NativeSchema nativeSchema = nsa.get(0);
			length = nativeSchema.getLength();
		}
	}

	@Override
	public Message convertTupleToMessage(Tuple tuple, Session session) throws JMSException,
			UnsupportedEncodingException, ParserConfigurationException, TransformerException {

		TextMessage textMessage;
		synchronized (session) {
			textMessage = session.createTextMessage();
		}

		MetaType attrType = tuple.getStreamSchema().getAttribute(0).getType().getMetaType();

		String msgText = ""; //$NON-NLS-1$

		if (attrType == MetaType.RSTRING || attrType == MetaType.USTRING) {
			// use getObject to avoid copying of the tuple
			Object tupleVal = tuple.getObject(0);
			if (tupleVal instanceof RString) {
				msgText = ((RString) tupleVal).getString();
			} else if (tupleVal instanceof String) {
				msgText = (String) tupleVal;
			}
		} else if (attrType == MetaType.XML) {
			XML xmlValue = tuple.getXML(0);
			msgText = xmlValue.toString();
		}

		// make sure message length is < the length specified in native schema
		if (length > 0 && msgText.length() > length) {
			msgText = msgText.substring(0, length);
			nTruncatedInserts.increment();
		}
		textMessage.setText(msgText);

		return textMessage;
	}

	@Override
	public MessageAction convertMessageToTuple(Message message, OutputTuple tuple) throws JMSException,
			UnsupportedEncodingException {

		TextMessage textMessage = (TextMessage) message;

		// make sure message length is < the length specified in native schema
		String tupleStr = textMessage.getText();
		if (length > 0 && tupleStr.length() > length) {
			tupleStr = tupleStr.substring(0, length);
		}

		MetaType attrType = tuple.getStreamSchema().getAttribute(0).getType().getMetaType();

		if (attrType == MetaType.RSTRING || attrType == MetaType.USTRING) {
			tuple.setString(0, tupleStr);
		} else if (attrType == MetaType.XML) {
			ByteArrayInputStream inputStream = new ByteArrayInputStream(tupleStr.getBytes());
			try {
				XML xmlValue = ValueFactory.newXML(inputStream);
				tuple.setXML(0, xmlValue);
			} catch (IOException e) {		
				// unable to convert incoming string to xml value
				// discard message and continue
				return MessageAction.DISCARD_MESSAGE_MESSAGE_FORMAT_ERROR;
			}
		}

		return MessageAction.SUCCESSFUL_MESSAGE;
	}

}
