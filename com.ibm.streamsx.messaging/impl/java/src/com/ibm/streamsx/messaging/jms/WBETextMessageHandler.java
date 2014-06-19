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
import javax.jms.TextMessage;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.metrics.Metric;

//This class handles the wbe22 message type 
class WBETextMessageHandler extends TextMessageHandler {
	/* begin_generated_IBM_copyright_code */
	public static final String IBM_COPYRIGHT = " Licensed Materials-Property of IBM                              " + //$NON-NLS-1$ 
			" 5724-Y95                                                        "
			+ //$NON-NLS-1$ 
			" (C) Copyright IBM Corp.  2013, 2013    All Rights Reserved.     "
			+ //$NON-NLS-1$ 
			" US Government Users Restricted Rights - Use, duplication or     "
			+ //$NON-NLS-1$ 
			" disclosure restricted by GSA ADP Schedule Contract with         "
			+ //$NON-NLS-1$ 
			" IBM Corp.                                                       "
			+ //$NON-NLS-1$ 
			"                                                                 "; //$NON-NLS-1$ 
	/* end_generated_IBM_copyright_code */

	// the document builder
	private DocumentBuilder documentBuilder;

	// constructor
	public WBETextMessageHandler(List<NativeSchema> nativeSchemaObjects,
			String eventName) throws TransformerConfigurationException,
			ParserConfigurationException {
		// call the base class constructor to initialize the native schema
		// attributes and event name.
		super(nativeSchemaObjects, eventName);
		documentBuilder = DocumentBuilderFactory.newInstance()
				.newDocumentBuilder();
	}

	// constructor
	public WBETextMessageHandler(List<NativeSchema> nativeSchemaObjects,
			String eventName, Metric nTruncatedInserts)
			throws TransformerConfigurationException,
			ParserConfigurationException {
		// call the base class constructor to initialize the native schema
		// attributes,nTruncatedInserts and event name
		super(nativeSchemaObjects, eventName, nTruncatedInserts);
		documentBuilder = DocumentBuilderFactory.newInstance()
				.newDocumentBuilder();
	}

	// For JMSSink operator, convert the incoming tuple to a JMS TextMessage
	public Message convertTupleToMessage(Tuple tuple, Session session)
			throws JMSException, ParserConfigurationException,
			TransformerException {
		// create a new TextMessage
		TextMessage message;
		synchronized (session) {
			message = (TextMessage) session.createTextMessage();
		}
		// create document element
		Document document;

		synchronized (documentBuilder) {
			document = documentBuilder.newDocument();
		}
		// String to hold attribute values
		String stringdata = new String();
		// variable to specify if any of the attributes in the message is
		// truncated
		boolean isTruncated = false;
		// create elements for constructing the xml document
		// with wbe mesage format

		Element rootElement = document.createElement("connector"); // creates a
																	// element
		// for root tuple
		rootElement.setAttribute("xmlns", "http://wbe.ibm.com/6.2/Event/"
				+ eventName);
		rootElement.setAttribute("name", "System S");
		rootElement.setAttribute("version", "6.2");
		Element rootEle2 = document.createElement("connector-bundle");
		rootEle2.setAttribute("name", eventName);
		rootEle2.setAttribute("type", "Event");
		rootElement.appendChild(rootEle2);
		Element rootEle3 = document.createElement(eventName);
		rootEle2.appendChild(rootEle3);

		for (NativeSchema currentObject : nativeSchemaObjects) {
			// iterate through the native schema elements
			// extract the name, type and length
			final String name = currentObject.getName();
			final int length = currentObject.getLength();
			Element ele = document.createElement(name);
			// handle based on data type
			switch (tuple.getStreamSchema().getAttribute(name).getType()
					.getMetaType()) {
			// BLOB is not supported for wbe22 message class
			case RSTRING:
			case USTRING:
				// extract the String
				// get its length
				String rdata = tuple.getString(name);
				int size = rdata.length();
				// If no length was specified in native schema or
				// if the length of the String rdata is less than the length
				// specified in native schema
				if (length == LENGTH_ABSENT_IN_NATIVE_SCHEMA || size <= length) {
					stringdata = rdata;
				}
				// if the length of rdate is greater than the length specified
				// in native schema
				// set the isTruncated to true
				// truncate the String
				else if (size > length) {
					isTruncated = true;
					stringdata = rdata.substring(0, length);
				}

				ele.setAttribute("data-type", "string");
				break;
			// spl types decimal32, decimal64,decimal128, timestamp are mapped
			// to String.
			case DECIMAL32:
			case DECIMAL64:
			case DECIMAL128:
				stringdata = tuple.getBigDecimal(name).toString();
				ele.setAttribute("data-type", "string");
				break;
			case TIMESTAMP:
				stringdata = tuple.getTimestamp(name).getTimeAsSeconds()
						.toString();
				ele.setAttribute("data-type", "string");
				break;
			case INT8:
			case UINT8:
			case INT16:
			case UINT16:
			case INT32:
			case UINT32:
			case INT64:
			case UINT64:
				stringdata = tuple.getString(name);
				ele.setAttribute("data-type", "integer");
				break;
			case FLOAT32:
			case FLOAT64:
				stringdata = tuple.getString(name);
				ele.setAttribute("data-type", "real");

				break;
			case BOOLEAN:
				if (tuple.getBoolean(name)) {
					stringdata = "true";

				} else {
					stringdata = "false";

				}
				ele.setAttribute("data-type", "boolean");

				break;
			}

			ele.appendChild(document.createTextNode((stringdata)));
			// append to root element
			rootEle3.appendChild(ele);
		}
		document.appendChild(rootElement); // add the rootElement to the
		// document
		// set the message
		message.setText(createFinalDocument(document));
		// if the isTruncated boolean is set, increment the metric
		// nTruncatedInserts
		if (isTruncated) {
			nTruncatedInserts.incrementValue(1);
		}
		// return the message
		return message;

	}

}
