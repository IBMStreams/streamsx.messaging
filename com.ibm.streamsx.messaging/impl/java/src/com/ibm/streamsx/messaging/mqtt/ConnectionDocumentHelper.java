/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/


package com.ibm.streamsx.messaging.mqtt;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Set;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.ibm.streams.operator.log4j.LogLevel;
import com.ibm.streams.operator.log4j.LoggerNames;
import com.ibm.streams.operator.log4j.TraceLevel;

//This class parses and validates the connections document 
class ConnectionDocumentHelper {
	
	private static Logger TRACE = Logger.getLogger(ConnectionDocumentHelper.class);
	private static final Logger LOG = Logger.getLogger(LoggerNames.LOG_FACILITY + "." + ConnectionDocumentHelper.class.getName()); //$NON-NLS-1$
	
	
	private static final String EMPTY_STRING = ""; //$NON-NLS-1$
	
	LinkedHashMap<String, ConnectionSpecification> connSpecMap = new LinkedHashMap<String, ConnectionSpecification>();

	public void parseAndValidateConnectionDocument(String connectionDocument)
			throws Exception, SAXException, IOException,
			ParserConfigurationException {
		try {
			// validate the connections document against the xsd
			validateConnectionsXML(connectionDocument);
			// create document builder
			Element docEle = createDocumentBuilder(connectionDocument);

			// parse validate the connection_specification tag in connections
			// document
			parseConnectionSpecifications(docEle);
		} catch (Exception e) {
			
			TRACE.log(TraceLevel.ERROR, Messages.getString("Error_ConnectionDocumentHelper.1"), e); //$NON-NLS-1$
			LOG.log(LogLevel.ERROR, Messages.getString("Error_ConnectionDocumentHelper.1"), e); //$NON-NLS-1$
			throw e;
		}

	}

	// subroutine to validate the connections document against the xsd
	public void validateConnectionsXML(String connectionDocument)
			throws SAXException, IOException {
		// validate against the xsd, if validation error occurs, it throws the
		// error and aborts at runtime
		SchemaFactory factory = SchemaFactory
				.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);

		// read schema from classpath
		InputStream resourceAsStream = getClass().getResourceAsStream(
				"mqttconnection.xsd"); //$NON-NLS-1$
		Source streamSource = new StreamSource(resourceAsStream);

		// create new schema
		Schema schema = factory.newSchema(streamSource);

		Validator validator = schema.newValidator();
		Source source = new StreamSource(connectionDocument);
		validator.validate(source);

	}

	// subroutine to create document builder
	private Element createDocumentBuilder(String connectionDocument)
			throws ParserConfigurationException, IOException, SAXException {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		Document doc = db.parse(connectionDocument);
		Element docEle = doc.getDocumentElement();
		return docEle;
	}

	private void parseConnectionSpecifications(Element element) {
		NodeList connectionSpecs = element
				.getElementsByTagName("connection_specification"); //$NON-NLS-1$

		int length = connectionSpecs.getLength();
		for (int i = 0; i < length; i++) {
			Node spec = connectionSpecs.item(i);
			NodeList childNodes = spec.getChildNodes();

			String name = getAttributeValue(spec.getAttributes(), "name"); //$NON-NLS-1$

			if (name != null) {
				int numChildren = childNodes.getLength();
				for (int j = 0; j < numChildren; j++) {
					Node connElement = childNodes.item(j);
					if (connElement.getNodeName().equals("MQTT")) { //$NON-NLS-1$
						NamedNodeMap attributes = connElement.getAttributes();
						if (attributes != null) {
							String serverUriStr = getAttributeValue(attributes,
									"serverURI"); //$NON-NLS-1$
							String trustStore = getAttributeValue(attributes,
									"trustStore"); //$NON-NLS-1$
							String keyStore = getAttributeValue(attributes,
									"keyStore"); //$NON-NLS-1$
							String keyStorePassword = getAttributeValue(
									attributes, "keyStorePassword"); //$NON-NLS-1$
							String trustStorePassword = getAttributeValue(attributes, "trustStorePassword"); //$NON-NLS-1$
							String userID = getAttributeValue(attributes, "userID");
							String password = getAttributeValue(attributes, "password");
							
							int keepAliveInterval;
							try {
								keepAliveInterval = Integer.parseInt(getAttributeValue(attributes, "keepAliveInterval"));
							} catch (NumberFormatException e) {
								keepAliveInterval = IMqttConstants.UNINITIALIZED_KEEP_ALIVE_INTERVAL;
							}
							
							long commandTimeout;
							try {
								commandTimeout = Long.parseLong(getAttributeValue(attributes, "commandTimeout"));
							} catch (NumberFormatException e) {
								commandTimeout = IMqttConstants.UNINITIALIZED_COMMAND_TIMEOUT;
							}
							
							if (!serverUriStr.isEmpty()) {
								ConnectionSpecification specObj = new ConnectionSpecification();
								specObj.setServerUri(serverUriStr)
										.setTrustStore(trustStore)
										.setKeyStore(keyStore)
										.setKeyStorePassword(keyStorePassword)
										.setTrustStorePassword(trustStorePassword)
										.setUserID(userID)
										.setPassword(password)
										.setKeepAliveInterval(keepAliveInterval)
										.setCommandTimeout(commandTimeout);

								connSpecMap.put(name, specObj);
							}
						}
					}
				}
			}
		}
	}

	private String getAttributeValue(NamedNodeMap attributes, String attrName) {
		Node attribute = attributes.getNamedItem(attrName);
		if (attribute != null) {
			return attribute.getNodeValue();
		}
		return EMPTY_STRING;
	}

	public ConnectionSpecification getConnectionSpecification(String name) {
		
		if (name == null)
		{
			// return the first connection
			if (!connSpecMap.isEmpty())
			{
				Set<String> keySet = connSpecMap.keySet();
				Iterator<String> keyIterator = keySet.iterator();
				if (keyIterator.hasNext())
				{
					return connSpecMap.get(keyIterator.next());
				}
			}
		}
		
		return connSpecMap.get(name);
	}
}
