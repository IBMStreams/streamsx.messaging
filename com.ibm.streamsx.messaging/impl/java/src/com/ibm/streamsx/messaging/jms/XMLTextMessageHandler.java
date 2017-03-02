/*******************************************************************************
 * Copyright (C) 2013, 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

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
import com.ibm.streams.operator.types.Blob;

//This class handles the wbe22 message type 
class XMLTextMessageHandler extends BaseXMLMessageHandler {
	
	// the document builder
	private DocumentBuilder documentBuilder;

	// constructor
	public XMLTextMessageHandler(List<NativeSchema> nativeSchemaObjects,
			String eventName) throws ParserConfigurationException,
			TransformerConfigurationException {
		// call the base class constructor to initialize the native schema
		// attributes and event name
		super(nativeSchemaObjects, eventName);
		documentBuilder = DocumentBuilderFactory.newInstance()
				.newDocumentBuilder();
	}

	// constructor
	public XMLTextMessageHandler(List<NativeSchema> nativeSchemaObjects,
			String eventName, Metric nTruncatedInserts)
			throws ParserConfigurationException,
			TransformerConfigurationException {
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

		// Since there are not thread safe
		synchronized (documentBuilder) {
			document = documentBuilder.newDocument();
		}
		// create elements for constructing the xml document
		// with spl xml data type format
		Element attr;
		// creates an element for root tuple
		Element rootElement = document.createElement("tuple"); //$NON-NLS-1$
		rootElement.setAttribute("xmlns", //$NON-NLS-1$
				"http://www.ibm.com/xmlns/prod/streams/spl/tuple"); //$NON-NLS-1$
		// variable to specify if any of the attributes in the message is
		// truncated
		boolean isTruncated = false;
		String stringdata = new String();

		for (NativeSchema currentObject : nativeSchemaObjects) {
			// iterate through the native schema elements
			// extract the name, type and length
			final String name = currentObject.getName();
			final int length = currentObject.getLength();

			attr = document.createElement("attr"); // create another //$NON-NLS-1$
			// element
			attr.setAttribute("name", name); //$NON-NLS-1$
			attr.setAttribute("type", tuple.getStreamSchema() //$NON-NLS-1$
					.getAttribute(name).getType().getLanguageType());

			// handle based on data type
			switch (tuple.getStreamSchema().getAttribute(name).getType()
					.getMetaType()) {
			case RSTRING:
			case USTRING: {
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
			}
				break;
			// spl types decimal32, decimal64,decimal128, timestamp are mapped
			// to String.
			case TIMESTAMP: {
				stringdata = (tuple.getTimestamp(name).getTimeAsSeconds())
						.toString();
			}
				break;
			case DECIMAL32:
			case DECIMAL64:
			case DECIMAL128: {
				stringdata = tuple.getBigDecimal(name).toString();
			}
				break;
			case BLOB: {
				// extract the blob
				// get its length
				Blob bl = tuple.getBlob(name);
				long size = bl.getLength();
				// if the length of the blob is greater than the length
				// specified in native schema
				// set the isTruncated to true
				// truncate the blob
				if (size > length && length != LENGTH_ABSENT_IN_NATIVE_SCHEMA) {
					isTruncated = true;
					size = length;
				}
				byte[] blobdata = new byte[(int) size];
				bl.getByteBuffer(0, (int) size).get(blobdata);
				// set the bytes into the messaage
				StringBuilder sb = new StringBuilder();
				for (byte b : blobdata)
					sb.append(String.format("%02x", b & 0xff)); //$NON-NLS-1$

				stringdata = sb.toString();

			}
				break;
			default:
				stringdata = tuple.getString(name);
				break;
			}

			attr.appendChild(document.createTextNode((stringdata)));
			rootElement.appendChild(attr); // add element1 under rootElement
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
		return message;

	}// convert end

}
