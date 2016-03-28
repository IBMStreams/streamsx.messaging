/*******************************************************************************
 * Copyright (C) 2013, 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.jms;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;

//This class parses and validates the connections document 
class ConnectionDocumentParser {

	// Variable to hold the supported SPL data types for the adapter
	private static final Set<String> supportedSPLTypes = new HashSet<String>(Arrays.asList("int8", "uint8", "int16",
			"uint16", "int32", "uint32", "int64", "float32", "float64", "boolean", "blob", "rstring", "uint64",
			"decimal32", "decimal64", "decimal128", "ustring", "timestamp", "xml"));

	// If the length is absent in native schema, we use -999
	static final int LENGTH_ABSENT_IN_NATIVE_SCHEMA = -999;

	// Variable to hold msgClass
	private MessageClass msgClass;
	// variables to connection related parameters in connection specification
	// document
	// context Factory
	private String initialContextFactory;
	// provider URL
	private String providerURL;
	// connection Factory
	private String connectionFactory;
	// destination
	private String destination;
	// delivery Mode can be persistent or non_persistent, default is persistent
	private String deliveryMode;
	// user Principal
	private String userPrincipal;
	// user Credentials
	private String userCredential;

	// variables to hold the native schema attributes which are specified in
	// connection document
	private List<NativeSchema> nativeSchemaObjects = new ArrayList<NativeSchema>();

	// Variable to create a mapping table for mapping between SPL datatypes and
	// their equivalent native schema data type for different message classes
	// mapping for JMSBytes Message class
	private final static HashMap<String, String> mapSPLToNativeSchemaDataTypesForBytes;
	// mapping for JMSText Message class
	private final static HashMap<String, String> mapSPLToNativeSchemaDataTypesForText;
	// mapping for other Message classes like JMSStream and JMS Map Message
	// class
	private final static HashMap<String, String> mapSPLToNativeSchemaDataTypesForOtherMsgClass;
	// static initializer blocks for above
	static {
		mapSPLToNativeSchemaDataTypesForBytes = new HashMap<String, String>();
		mapSPLToNativeSchemaDataTypesForOtherMsgClass = new HashMap<String, String>();
		mapSPLToNativeSchemaDataTypesForText = new HashMap<String, String>();
		// method to populate mapSPLToNativeSchemaDataTypesForOtherMsgClass

		mapSPLToNativeSchemaDataTypesForOtherMsgClass.put("int8", "Byte");
		mapSPLToNativeSchemaDataTypesForOtherMsgClass.put("uint8", "Byte");
		mapSPLToNativeSchemaDataTypesForOtherMsgClass.put("int16", "Short");
		mapSPLToNativeSchemaDataTypesForOtherMsgClass.put("uint16", "Short");
		mapSPLToNativeSchemaDataTypesForOtherMsgClass.put("int32", "Int");
		mapSPLToNativeSchemaDataTypesForOtherMsgClass.put("uint32", "Int");
		mapSPLToNativeSchemaDataTypesForOtherMsgClass.put("int64", "Long");
		mapSPLToNativeSchemaDataTypesForOtherMsgClass.put("uint64", "Long");
		mapSPLToNativeSchemaDataTypesForOtherMsgClass.put("float32", "Float");
		mapSPLToNativeSchemaDataTypesForOtherMsgClass.put("float64", "Double");
		mapSPLToNativeSchemaDataTypesForOtherMsgClass.put("boolean", "Boolean");
		mapSPLToNativeSchemaDataTypesForOtherMsgClass.put("blob", "Bytes");
		mapSPLToNativeSchemaDataTypesForOtherMsgClass.put("rstring", "String");
		mapSPLToNativeSchemaDataTypesForOtherMsgClass.put("ustring", "String");
		mapSPLToNativeSchemaDataTypesForOtherMsgClass.put("decimal32", "String");
		mapSPLToNativeSchemaDataTypesForOtherMsgClass.put("decimal64", "String");
		mapSPLToNativeSchemaDataTypesForOtherMsgClass.put("decimal128", "String");
		mapSPLToNativeSchemaDataTypesForOtherMsgClass.put("timestamp", "String");
		mapSPLToNativeSchemaDataTypesForOtherMsgClass.put("xml", "String");

		mapSPLToNativeSchemaDataTypesForBytes.put("int8", "Byte");
		mapSPLToNativeSchemaDataTypesForBytes.put("uint8", "Byte");
		mapSPLToNativeSchemaDataTypesForBytes.put("int16", "Short");
		mapSPLToNativeSchemaDataTypesForBytes.put("uint16", "Short");
		mapSPLToNativeSchemaDataTypesForBytes.put("int32", "Int");
		mapSPLToNativeSchemaDataTypesForBytes.put("uint32", "Int");
		mapSPLToNativeSchemaDataTypesForBytes.put("int64", "Long");
		mapSPLToNativeSchemaDataTypesForBytes.put("uint64", "Long");
		mapSPLToNativeSchemaDataTypesForBytes.put("float32", "Float");
		mapSPLToNativeSchemaDataTypesForBytes.put("float64", "Double");
		mapSPLToNativeSchemaDataTypesForBytes.put("boolean", "Boolean");
		mapSPLToNativeSchemaDataTypesForBytes.put("blob", "Bytes");
		mapSPLToNativeSchemaDataTypesForBytes.put("rstring", "Bytes");
		mapSPLToNativeSchemaDataTypesForBytes.put("ustring", "Bytes");
		mapSPLToNativeSchemaDataTypesForBytes.put("decimal32", "Bytes");
		mapSPLToNativeSchemaDataTypesForBytes.put("decimal64", "Bytes");
		mapSPLToNativeSchemaDataTypesForBytes.put("decimal128", "Bytes");
		mapSPLToNativeSchemaDataTypesForBytes.put("timestamp", "Bytes");
		mapSPLToNativeSchemaDataTypesForBytes.put("xml", "Bytes");

		mapSPLToNativeSchemaDataTypesForText.put("int8", "String");
		mapSPLToNativeSchemaDataTypesForText.put("uint8", "String");
		mapSPLToNativeSchemaDataTypesForText.put("int16", "String");
		mapSPLToNativeSchemaDataTypesForText.put("uint16", "String");
		mapSPLToNativeSchemaDataTypesForText.put("int32", "String");
		mapSPLToNativeSchemaDataTypesForText.put("uint32", "String");
		mapSPLToNativeSchemaDataTypesForText.put("int64", "String");
		mapSPLToNativeSchemaDataTypesForText.put("uint64", "String");
		mapSPLToNativeSchemaDataTypesForText.put("float32", "String");
		mapSPLToNativeSchemaDataTypesForText.put("float64", "String");
		mapSPLToNativeSchemaDataTypesForText.put("boolean", "String");
		mapSPLToNativeSchemaDataTypesForText.put("blob", "String");
		mapSPLToNativeSchemaDataTypesForText.put("rstring", "String");
		mapSPLToNativeSchemaDataTypesForText.put("ustring", "String");
		mapSPLToNativeSchemaDataTypesForText.put("decimal32", "String");
		mapSPLToNativeSchemaDataTypesForText.put("decimal64", "String");
		mapSPLToNativeSchemaDataTypesForText.put("decimal128", "String");
		mapSPLToNativeSchemaDataTypesForText.put("timestamp", "String");
		mapSPLToNativeSchemaDataTypesForText.put("xml", "String");

	}

	// getter for NativeSchemaObjects
	public List<NativeSchema> getNativeSchemaObjects() {
		return nativeSchemaObjects;
	}

	// getter for initialContextFactory
	public String getInitialContextFactory() {
		return initialContextFactory;
	}

	// getter for providerURL
	public String getProviderURL() {
		return providerURL;
	}

	// getter for destination
	public String getDestination() {
		return destination;
	}

	// getter for DeliveryMode
	public String getDeliveryMode() {
		return deliveryMode;
	}

	// getter for userPrinical
	public String getUserPrincipal() {
		return userPrincipal;
	}

	// getter for userCredential
	public String getUserCredential() {
		return userCredential;
	}

	// getter for connectionFactory
	public String getConnectionFactory() {
		return connectionFactory;
	}

	// getter for MessageType
	public MessageClass getMessageType() {
		return msgClass;

	}
	
	// Convert relative provider url path to absolute path for wmq only.
	// non-absolute path should be relative to application directory.
	// i.e file:./etc/ will be converted to applicationDir + ./etc/
	private void convertProviderURLPath(File applicationDir) throws ParseConnectionDocumentException {
		
	   if(!isAMQ()) {
		   
		   // provider_url can not be empty
		   if(this.providerURL == null || this.providerURL.trim().length() == 0) { 
			   throw new ParseConnectionDocumentException("A value must be specified for provider_url attribute in connection document");
		   }
		   
		   // provider_url has a value specified
		   try {
		       URL url = new URL(providerURL);
		       
		       // We only care about url with file scheme.
		       if("file".equalsIgnoreCase(url.getProtocol())) {
		    	   String path = url.getPath();
		    	   
		    	   // relative path is considered being relative to the application directory
		    	   if(!path.startsWith("/")) {
				          URL absProviderURL = new URL(url.getProtocol(), url.getHost(), applicationDir.getAbsolutePath() + File.separator + path);
				    	  this.providerURL = absProviderURL.toExternalForm();
				      }
		       }
		       
		   } catch (MalformedURLException e) {
			   throw new ParseConnectionDocumentException("Invalid provider_url value detected: " + e.getMessage());
		   }
		   
	   }
	}

	// subroutine to parse and validate the connection document
	// called by both the JMSSink and JMSSource
	public void parseAndValidateConnectionDocument(String connectionDocument, String connection, String access,
			StreamSchema streamSchema, boolean isProducer, File applicationDir) throws ParseConnectionDocumentException, SAXException,
			IOException, ParserConfigurationException {
		// validate the connections document against the xsd
		validateConnectionsXML(connectionDocument);
		// create document builder
		Element docEle = createDocumentBuilder(connectionDocument);
		// parse validate the connection_specification tag in connections
		// document
		parseConnSpecElement(connection, docEle);
		// parse validate the access_specification tag in connections document
		Node nativeSchema = parseAccessSpecElement(connection, access, docEle);
		// perform validations common to JMSSink and JMSSource
		connDocChecksCommonToSourceSink(streamSchema, nativeSchema);
		// perform validations for JMSSource
		if (!isProducer) {
			connDocChecksSource(streamSchema);
		}
		// perform native schema validations
		if (msgClass != MessageClass.empty) {
			nativeSchemaChecks(isProducer, streamSchema, nativeSchema);
		}
		// convert provider_url to absolute if needed
		convertProviderURLPath(applicationDir);
	}

	// subroutine to validate the connections document against the xsd
	private void validateConnectionsXML(String connectionDocument) throws SAXException, IOException {
		// validate against the xsd, if validation error occurs, it throws the
		// error and aborts at runtime
		SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);

		// read schema from classpath
		InputStream resourceAsStream = getClass().getResourceAsStream("jmsconnection.xsd"); //$NON-NLS-1$
		Source streamSource = new StreamSource(resourceAsStream);
		Schema schema = factory.newSchema(streamSource);
		Validator validator = schema.newValidator();
		Source source = new StreamSource(connectionDocument);
		validator.validate(source);
	}

	// subroutine to create document builder
	private Element createDocumentBuilder(String connectionDocument) throws ParserConfigurationException, IOException,
			SAXException {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		Document doc = db.parse(connectionDocument);
		Element docEle = doc.getDocumentElement();
		return docEle;
	}

	// subroutine to parse validate the connection_specification tag in
	// connections document
	private void parseConnSpecElement(String connection, Element docEle) throws ParseConnectionDocumentException {
		// variable to specify if the connection_specification name as specified
		// in the connection parameter of the operator model is found.
		// set to true if its found, false otherwise
		boolean connectionFound = false;
		// extract the connection_specification elements
		NodeList connection_specification = docEle.getElementsByTagName("connection_specification");
		// iterate through the list to verify if the connection_specification
		// with value as connection exists
		for (int i = 0; i < connection_specification.getLength(); i++) {
			if (connection.equals(connection_specification.item(i).getAttributes().getNamedItem("name").getNodeValue())) {
				// found connection_specification
				int jmsIndex = -1;
				// Extract the child nodes
				NodeList connSpecChildNodes = connection_specification.item(i).getChildNodes();
				// verify if it has a JMS tag
				for (int j = 0; j < connSpecChildNodes.getLength(); j++) {
					if (connSpecChildNodes.item(j).getNodeName().equals("JMS")) {
						jmsIndex = j;
						break;
					}
				}
				// extract the jms element
				Node destination = connSpecChildNodes.item(jmsIndex);
				// extract the provider URL from the JMS element
				providerURL = destination.getAttributes().getNamedItem("provider_url").getNodeValue();
				// extract the initialContext from the JMS element
				initialContextFactory = destination.getAttributes().getNamedItem("initial_context").getNodeValue();
				// extract the connectionFactory from the JMS element
				connectionFactory = destination.getAttributes().getNamedItem("connection_factory").getNodeValue();

				// check if optional elements user and password are specified
				// if speciifed extract those
				if (destination.getAttributes().getNamedItem("user") != null) {
					userPrincipal = destination.getAttributes().getNamedItem("user").getNodeValue();
				}
				if (destination.getAttributes().getNamedItem("password") != null) {
					userCredential = destination.getAttributes().getNamedItem("password").getNodeValue();
				}
				// Verify if either both user and password are present or none
				// is present
				// throw a ParseConnectionDocumentException otherwise
				if (userPrincipal == null && userCredential != null || userPrincipal != null && userCredential == null) {
					throw new ParseConnectionDocumentException("Only one of userPrinicpal, userCredential is set.");
				}
				// set the connectionFound to true
				connectionFound = true;

				break;
			}
		}
		// throw ParseConnectionDocumentException if the connection value
		// specified in the parameter is not found in the connection document
		if (!connectionFound) {
			throw new ParseConnectionDocumentException("The value of the connection parameter " + connection
					+ " is not found in the connections document");

		}
		return;
	}

	// subroutine to parse validate the access_specification tag in connections
	// document
	private Node parseAccessSpecElement(String connection, String access, Element docEle)
			throws ParseConnectionDocumentException {
		// variable to specify if the access_specification name as specified in
		// the access parameter of the operator model is found.
		// set to true if its found, false otherwise
		boolean accessFound = false;
		// native schema attribute list
		Node nativeSchema = null;
		// extract the access_specification node list
		NodeList access_specification = docEle.getElementsByTagName("access_specification");
		// iterate through the node list to find the access_specification
		// element with value as access paramter
		for (int i = 0; i < access_specification.getLength(); i++) {
			if (access.equals(access_specification.item(i).getAttributes().getNamedItem("name").getNodeValue())) {
				// access_specification element found
				accessFound = true;

				int destIndex = -1;
				int nativeSchemaIndex = -1;
				// get the child nodes
				NodeList accessSpecChildNodes = access_specification.item(i).getChildNodes();
				// iterate througgh the child nodes to find all the required
				// elements
				for (int j = 0; j < accessSpecChildNodes.getLength(); j++) {
					if (accessSpecChildNodes.item(j).getNodeName().equals("destination")) {
						// extract destination
						destIndex = j;
					} else if (accessSpecChildNodes.item(j).getNodeName().equals("uses_connection")) {
						// check if the uses_connection uses the same
						// connections as specified
						// in the SPL under connection parameter. Else throw
						// ParseConnectionDocumentException
						if (!connection.equals(accessSpecChildNodes.item(j).getAttributes().getNamedItem("connection")
								.getNodeValue())) {
							throw new ParseConnectionDocumentException("The value of the connection parameter "
									+ connection + " is not the same as the connection used by access element "
									+ access + " as mentioned in the uses_connection element in connections document");
						}
					} else if (accessSpecChildNodes.item(j).getNodeName().equals("native_schema")) {
						nativeSchemaIndex = j;
					}
				}
				String messageClass;
				// get the destination node
				Node dest = accessSpecChildNodes.item(destIndex);
				// get the destination value
				destination = dest.getAttributes().getNamedItem("identifier").getNodeValue();
				// get the message class
				messageClass = dest.getAttributes().getNamedItem("message_class").getNodeValue();
				// get the delivery mode if its present
				if (dest.getAttributes().getNamedItem("delivery_mode") != null) {
					deliveryMode = dest.getAttributes().getNamedItem("delivery_mode").getNodeValue();
				}
				// Extract the message class
				msgClass = MessageClass.valueOf(messageClass.trim());
				// get the native schema node list, it will not be present for
				// empty message class
				if (accessSpecChildNodes.item(nativeSchemaIndex) != null) {
					nativeSchema = access_specification.item(i).getChildNodes().item(nativeSchemaIndex);
				}
				break;
			}
		}
		// if accessFound is false , throw ParseConnectionDocumentException
		if (!accessFound) {
			throw new ParseConnectionDocumentException("The value of the access parameter " + access
					+ " is not found in the connections document");
		}

		return nativeSchema;
	}

	// subroutine to perform validations common for JMSSource and JMSSink
	private void connDocChecksCommonToSourceSink(StreamSchema streamSchema, Node nativeSchema)
			throws ParseConnectionDocumentException {
		// Native schema should be present for all message classes except empty
		// throw ParseConnectionDocumentException otherwise
		if (nativeSchema == null && msgClass != MessageClass.empty) {
			throw new ParseConnectionDocumentException("Native schema needs to be specified for message class "
					+ msgClass);
		}
		// Native schema should not be present for message class empty
		// if present throw ParseConnectionDocumentException
		if (nativeSchema != null && msgClass == MessageClass.empty) {
			throw new ParseConnectionDocumentException("Native schema cannot be specified with message class Empty ");

		}
		// check if attributes in streamschema are of supported type
		// or not
		for (Attribute attr : streamSchema) {
			String streamAttrName = attr.getName();
			MetaType streamAttrMetaType = attr.getType().getMetaType();
			if (!supportedSPLTypes.contains(streamAttrMetaType.getLanguageType())) {
				throw new ParseConnectionDocumentException("Attribute Type: " + streamAttrMetaType.getLanguageType()
						+ " For stream attribute: " + streamAttrName + " is not a supported SPL type ");

			}
		}
		return;
	}

	// subroutine to perform validations for JMSSource
	private void connDocChecksSource(StreamSchema streamSchema) throws ParseConnectionDocumentException {
		// Message classes xml, wbe and wbe22 are not allowed for JMSSource
		// throw ParseConnectionDocumentException if specified
		if (msgClass == MessageClass.xml || msgClass == MessageClass.wbe || msgClass == MessageClass.wbe22) {
			throw new ParseConnectionDocumentException(
					"Message classes xml, wbe and wbe22 are not supported for JMSSource");
		}

		for (Attribute attr : streamSchema) {
			MetaType streamAttrMetaType = attr.getType().getMetaType();
			// If the adapter is JMSSource, we dont support blob
			// type

			if (streamAttrMetaType == Type.MetaType.BLOB) {
				throw new ParseConnectionDocumentException("BLOB is not a supported SPL type for JMSSource adapter");
			}
		}
		return;
	}

	// subroutine to perform native schema validations
	private void nativeSchemaChecks(boolean isProducer, StreamSchema streamSchema, Node nativeSchema)
			throws ParseConnectionDocumentException {

		// get the attribute list
		NodeList attrList = nativeSchema.getChildNodes();

		for (int i = 0; i < attrList.getLength(); i++) {
			if (attrList.item(i).hasAttributes()) {
				// extract the native schema attribute name, type and length
				String nativeAttrName = (attrList.item(i).getAttributes().getNamedItem("name").getNodeValue());
				String nativeAttrType = (attrList.item(i).getAttributes().getNamedItem("type").getNodeValue());
				int nativeAttrLength;

				// if length is not specified for that parameter
				if (attrList.item(i).getAttributes().getNamedItem("length") == null) {
					nativeAttrLength = LENGTH_ABSENT_IN_NATIVE_SCHEMA;

				} else {
					nativeAttrLength = Integer.parseInt((attrList.item(i).getAttributes().getNamedItem("length")
							.getNodeValue()));

				}
				// Do not support blob data type if message class is wbe, wbe22
				if ((msgClass == MessageClass.wbe || msgClass == MessageClass.wbe22)
						&& ((streamSchema.getAttribute(nativeAttrName) != null) && (streamSchema
								.getAttribute(nativeAttrName).getType().getMetaType() == Type.MetaType.BLOB))) {
					throw new ParseConnectionDocumentException(" Blob data type is not supported for message class "
							+ msgClass);
				}

				// validate that the attribute name is not already
				// existing in the native schema
				Iterator<NativeSchema> it = nativeSchemaObjects.iterator();
				while (it.hasNext()) {
					if (it.next().getName().equals(nativeAttrName)) {

						throw new ParseConnectionDocumentException("Parameter name: " + nativeAttrName
								+ " is appearing more than once In native schema file");
					}
				}

				// set to hold the data types which can have length specified in
				// native schema
				Set<String> typesWithLength = new HashSet<String>(Arrays.asList("String", "Bytes"));

				// if message class is text, length on String attribute type is
				// optional
				if (msgClass == MessageClass.text) {
					typesWithLength = new HashSet<String>(Arrays.asList("Bytes"));
				}

				// set to hold the data types which can not have length
				// specified in native schema
				Set<String> typesWithoutLength = new HashSet<String>(Arrays.asList("Byte", "Short", "Int", "Long",
						"Float", "Double", "Boolean"));
				// Length value should not be present if the data type belongs
				// to typesWithoutLength
				if (typesWithoutLength.contains(nativeAttrType) && nativeAttrLength != LENGTH_ABSENT_IN_NATIVE_SCHEMA) {

					throw new ParseConnectionDocumentException("Length attribute should not be present for parameter: "
							+ nativeAttrName + " In native schema file");
				}

				// Since for xml, wbe and wbe22 all the String, a new check is
				// required

				if ((nativeAttrLength != LENGTH_ABSENT_IN_NATIVE_SCHEMA)
						&& (msgClass == MessageClass.wbe || msgClass == MessageClass.wbe22 || msgClass == MessageClass.xml)
						&& (streamSchema.getAttribute(nativeAttrName) != null)
						&& (streamSchema.getAttribute(nativeAttrName).getType().getMetaType() != Type.MetaType.RSTRING)
						&& (streamSchema.getAttribute(nativeAttrName).getType().getMetaType() != Type.MetaType.USTRING)
						&& (streamSchema.getAttribute(nativeAttrName).getType().getMetaType() != Type.MetaType.BLOB)) {

					throw new ParseConnectionDocumentException("Length attribute should not be present for parameter: "
							+ nativeAttrName + " In native schema file");
				}
				// Since decimal32, decimal64, decimal128 and timestamp are
				// mapped to bytes for message class bytes
				// and in message class bytes, since String/Bytes expect a
				// length, we, set a default length of -4.
				// Add a check that Streamschema has this particular native
				// schema attribute
				if (streamSchema.getAttribute(nativeAttrName) != null) {
					MetaType metaType = streamSchema.getAttribute(nativeAttrName).getType().getMetaType();
					if (metaType == Type.MetaType.DECIMAL32 || metaType == Type.MetaType.DECIMAL64
							|| metaType == Type.MetaType.DECIMAL128 || metaType == Type.MetaType.TIMESTAMP) {
						if (nativeAttrLength != LENGTH_ABSENT_IN_NATIVE_SCHEMA) {
							throw new ParseConnectionDocumentException(
									"Length attribute should not be present for parameter: " + nativeAttrName
											+ " with type " + metaType + " in native schema file.");
						}

						if (msgClass == MessageClass.bytes) {
							nativeAttrLength = -4;
						}
					}
				}
				// Length value should be present if the data type belongs to
				// typesWithLength and message class is bytes
				if (typesWithLength.contains(nativeAttrType)) {
					if (nativeAttrLength == LENGTH_ABSENT_IN_NATIVE_SCHEMA && msgClass == MessageClass.bytes) {
						throw new ParseConnectionDocumentException("Length attribute should be present for parameter: "
								+ nativeAttrName + " In native schema file for message class bytes");
					}
					// Length attribute can be non negative -2,-4 only for
					// message class bytes
					if ((nativeAttrLength < 0) && nativeAttrLength != LENGTH_ABSENT_IN_NATIVE_SCHEMA) {
						if (msgClass != MessageClass.bytes) {

							throw new ParseConnectionDocumentException(
									"Length attribute can be non negative -2,-4 only for message class bytes for parameter: "
											+ nativeAttrName + " In native schema file");

						}
						// If the Length attribute is non negative, it can only
						// be -2,-4 for message class bytes
						if (nativeAttrLength != -2 && nativeAttrLength != -4) {

							throw new ParseConnectionDocumentException(
									"Length attribute should be non negative or -2,-4 for parameter: " + nativeAttrName
											+ " In native schema file");

						}
					}
				}

				// validate if the input port stream schema contains this
				// attributes from the
				// native schema file

				if (streamSchema.getAttribute(nativeAttrName) == null && isProducer == true) {
					// for JMSSink , each and every attribute in
					// native schema
					// should be present in input stream

					throw new ParseConnectionDocumentException("Attribute Name: " + nativeAttrName + " with type:"
							+ nativeAttrType + " in the native schema cannot be found in stream schema ");
				}
				// Here we are comparing the data type of the native schema
				// attribute with the stream schema attribute of the same name
				// and throw an error if a mismatch happens
				String streamAttrName;
				MetaType streamAttrMetaType = null;

				if (streamSchema.getAttribute(nativeAttrName) != null) {
					streamAttrName = streamSchema.getAttribute(nativeAttrName).getName();
					streamAttrMetaType = streamSchema.getAttribute(nativeAttrName).getType().getMetaType();
					// for message classes map and stream
					if ((msgClass == MessageClass.stream || msgClass == MessageClass.map)
							&& !mapSPLToNativeSchemaDataTypesForOtherMsgClass.get(streamAttrMetaType.getLanguageType())
									.equals(nativeAttrType)) {

						throw new ParseConnectionDocumentException("Attribute Name: " + nativeAttrName + " with type:"
								+ nativeAttrType + " in the native schema cannot be mapped with attribute: "
								+ streamAttrName + " with type : " + streamAttrMetaType.getLanguageType());
					}
					// for message class bytes
					else if (msgClass == MessageClass.bytes
							&& !mapSPLToNativeSchemaDataTypesForBytes.get(streamAttrMetaType.getLanguageType()).equals(
									nativeAttrType)) {

						throw new ParseConnectionDocumentException("Attribute Name: " + nativeAttrName + " with type:"
								+ nativeAttrType + " in the native schema cannot be mapped with attribute: "
								+ streamAttrName + " with type : " + streamAttrMetaType.getLanguageType());
					}
					// for message classes xml,wbe,wbe22
					else if ((msgClass == MessageClass.wbe || msgClass == MessageClass.wbe22 || msgClass == MessageClass.xml)
							&& !mapSPLToNativeSchemaDataTypesForText.get(streamAttrMetaType.getLanguageType()).equals(
									nativeAttrType)) {

						throw new ParseConnectionDocumentException("Attribute Name: " + nativeAttrName + " with type:"
								+ nativeAttrType + " in the native schema cannot be mapped with attribute: "
								+ streamAttrName + " with type : " + streamAttrMetaType.getLanguageType());
					} else if (msgClass == MessageClass.text) {
						if (streamAttrMetaType != MetaType.RSTRING && streamAttrMetaType != MetaType.USTRING
								&& streamAttrMetaType != MetaType.XML) {

							throw new ParseConnectionDocumentException(streamAttrName
									+ " in spl schema must be of type rstring, ustring or xml");
						}
						
						if (!nativeAttrType.equals("String"))
						{
							throw new ParseConnectionDocumentException("Attribute Name: " + nativeAttrName + " with type:"
									+ nativeAttrType + " is invalid.  Attribute must be of type String.");
						}
					}
				}

				// the native schema parameter is valid, add to list
				NativeSchema currentObject;
				// if the paramter is present in streams schema, set the
				// isPresentInStreamSchema to true
				// else set it ti false
				if (streamSchema.getAttribute(nativeAttrName) == null) {
					currentObject = new NativeSchema(nativeAttrName, NativeTypes.valueOf(nativeAttrType),
							nativeAttrLength, false);
				} else {
					currentObject = new NativeSchema(nativeAttrName, NativeTypes.valueOf(nativeAttrType),
							nativeAttrLength, true);
				}
				nativeSchemaObjects.add(currentObject);

			}
		}

		// additional checks for message class text
		if (msgClass == MessageClass.text) {
			// for message class text, only allow one attribute on native schema
			if (nativeSchemaObjects.size() != 1) {
				throw new ParseConnectionDocumentException(
						"Native schema cannot contain more than one attribute with message class text.");
			}
		}

		return;
	}

	// subroutine to verify if the JMSProvider is Active MQ
	public boolean isAMQ() {
		if (initialContextFactory.contains("activemq")) {
			return true;
		}

		return false;

	}
}
