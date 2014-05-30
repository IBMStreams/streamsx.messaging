package com.ibm.streamsx.messaging.mqtt;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

//This class parses and validates the connections document 
class ConnectionDocumentHelper {

	private static final String EMPTY_STRING = "";
	HashMap<String, ConnectionSpecification> connSpecMap = new HashMap<>();

	public void parseAndValidateConnectionDocument(String connectionDocument)
			throws Exception, SAXException, IOException,
			ParserConfigurationException {
		// validate the connections document against the xsd
		validateConnectionsXML(connectionDocument);
		// create document builder
		Element docEle = createDocumentBuilder(connectionDocument);

		// parse validate the connection_specification tag in connections
		// document
		parseConnectionSpecifications(docEle);

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
				"connection.xsd");
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
				.getElementsByTagName("connection_specification");

		int length = connectionSpecs.getLength();
		for (int i = 0; i < length; i++) {
			Node spec = connectionSpecs.item(i);
			NodeList childNodes = spec.getChildNodes();

			String name = getAttributeValue(spec.getAttributes(), "name");

			if (name != null) {
				int numChildren = childNodes.getLength();
				for (int j = 0; j < numChildren; j++) {
					Node connElement = childNodes.item(j);
					if (connElement.getNodeName().equals("MQTT")) {
						NamedNodeMap attributes = connElement.getAttributes();
						if (attributes != null) {
							String serverUriStr = getAttributeValue(attributes,
									"serverURI");
							String trustStore = getAttributeValue(attributes,
									"trustStore");
							String keyStore = getAttributeValue(attributes,
									"keyStore");
							String keyStorePassword = getAttributeValue(
									attributes, "keyStorePassword");

							if (!serverUriStr.isEmpty()) {
								ConnectionSpecification specObj = new ConnectionSpecification();
								specObj.setServerUri(serverUriStr)
										.setTrustStore(trustStore)
										.setKeyStore(keyStore)
										.setKeyStorePassword(keyStorePassword);

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
				connSpecMap.values().iterator().next();
			}
		}
		
		return connSpecMap.get(name);
	}
}
