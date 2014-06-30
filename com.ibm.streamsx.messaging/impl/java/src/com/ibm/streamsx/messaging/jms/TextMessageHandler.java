/*******************************************************************************
 * Copyright (C) 2013, 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.messaging.jms;

import java.io.StringWriter;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.metrics.Metric;

abstract class TextMessageHandler extends JMSMessageHandlerImpl {

	// variable to hold event name, this is required for the wbe and wbe22
	// message classes
	protected final String eventName;
	// Transformer variable required to convert to xml type
	private Transformer transformer;

	// subroutine to construct the final xml ducument from the Document, it
	// throws ParserConfigurationException and TransformerException
	public String createFinalDocument(Document document)
			throws TransformerException {
		// ccreate the DOMSource
		DOMSource source = new DOMSource(document);
		// create the writer
		StringWriter swriter = new StringWriter();
		StreamResult result = new StreamResult(swriter);

		// The transformer is not thread safe. From the documentation:
		// An object of this class may not be used in multiple threads running
		// concurrently. Different Transformers may be used concurrently by
		// different threads.
		// Hence synchronizing this
		synchronized (transformer) {
			transformer.transform(source, result);
		}
		return (swriter.toString());
	}

	// constructor
	public TextMessageHandler(List<NativeSchema> nativeSchemaObjects,
			String eventName) throws TransformerConfigurationException {
		// call the base class constructor to initialize the native schema
		// attributes.
		super(nativeSchemaObjects);
		// set the event name, this is required for the wbe and wbe22 message
		// classes
		this.eventName = eventName;
		this.transformer = TransformerFactory.newInstance().newTransformer();
	}

	// constructor
	public TextMessageHandler(List<NativeSchema> nativeSchemaObjects,
			String eventName, Metric nTruncatedInserts)
			throws TransformerConfigurationException {
		// call the base class constructor to initialize the native schema
		// attributes.
		super(nativeSchemaObjects, nTruncatedInserts);
		// set the event name, this is required for the wbe and wbe22 message
		// classes
		this.eventName = eventName;
		this.transformer = TransformerFactory.newInstance().newTransformer();
	}

	// Currently we do not support the wbe, wbe22, xml message class for
	// JMSSource so this subroutine will never be called
	public MessageAction convertMessageToTuple(Message message,
			OutputTuple tuple) throws JMSException {
		// will not reach here
		return MessageAction.SUCCESSFUL_MESSAGE;
	}

}