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

import java.io.StringWriter;
import java.util.List;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Document;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.metrics.Metric;

abstract class TextMessageHandler extends JMSMessageHandlerImpl {
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