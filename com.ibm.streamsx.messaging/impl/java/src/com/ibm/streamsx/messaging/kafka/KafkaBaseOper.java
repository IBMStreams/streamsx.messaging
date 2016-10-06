/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.messaging.kafka;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

import javax.security.auth.login.Configuration;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streamsx.messaging.common.PropertyProvider;

/**
 * Base operator for all the common functions
 * 
 */
@Libraries({ "opt/downloaded/*","opt/*" })
public abstract class KafkaBaseOper extends AbstractOperator {

	protected Properties properties = new Properties(),
			finalProperties = new Properties(),
			appConfigProperties = new Properties();
    // application configuration name
	protected String appConfigName;
	protected String propertiesFile = null;
	protected AttributeHelper topicAH = new AttributeHelper("topic"),
			keyAH = new AttributeHelper("key"),
			messageAH = new AttributeHelper("message");
	protected List<String> topics = new ArrayList<String>();
	private List<String> appConfigPropName = new ArrayList<String>();
	private String jaasFile = null;
	private String jaasFilePropName = "jaasFile";
	private static final Logger trace = Logger.getLogger(KafkaBaseOper.class
			.getCanonicalName());

	/*
	 * Check that either appConfig, propertiesFile, or kafkaProperty parameter specified. 
	 */
	@ContextCheck(runtime = false, compile = true)
	public static void checkCompileCompatability(OperatorContextChecker checker) {
		OperatorContext operContext = checker.getOperatorContext();

		if (!operContext.getParameterNames().contains("propertiesFile")
				&& !operContext.getParameterNames().contains("kafkaProperty")
				&& !operContext.getParameterNames().contains("appConfigName")) {
			checker.setInvalidContext(
					"Missing properties: Check that appConfig, propertiesFile, or kafkaProperty parameter specified. At least one must be set.",
					new String[] {});

		}
		
		checker.checkDependentParameters("jaasFilePropName", "appConfigName");

	}
	
	/*
	 * The method checkParametersRuntime validates that the reconnection policy
	 * parameters are appropriate
	 */
	@ContextCheck(compile = false)
	public static void checkParametersRuntime(OperatorContextChecker checker) {
		if((checker.getOperatorContext().getParameterNames().contains("appConfigName"))) {
        	String appConfigName = checker.getOperatorContext().getParameterValues("appConfigName").get(0);
			List<String> appConfigPropName = checker.getOperatorContext().getParameterValues("appConfigPropertyName");
			
			PropertyProvider provider = new PropertyProvider(checker.getOperatorContext().getPE(), appConfigName);
			for (String propName : appConfigPropName){
				String prop = provider.getProperty(propName);
				if(prop == null || prop.trim().length() == 0) {
					trace.log(TraceLevel.ERROR, "Property " + propName + " is not found in application configuration " + appConfigName);
					checker.setInvalidContext(
							"Property {0} is not found in application configuration {1}.",
							new Object[] {propName, appConfigName});
					
				} else {
					trace.log(TraceLevel.TRACE, "Property " + propName + " was found in the appConfig: " + appConfigName);
				}
			}
        }
	}

	
	protected static void checkForMessageAttribute(OperatorContext operContext, StreamSchema operSchema) throws Exception {
		List<String> messageAttrParameter = operContext.getParameterValues("messageAttribute");
		String messageAttrString = "message";
		if (!messageAttrParameter.isEmpty()){
			messageAttrString = messageAttrParameter.get(0);
		}
		if ( !operSchema.getAttributeNames().contains(messageAttrString)
				|| !operSchema.getAttributeNames().contains(messageAttrString)){
			throw new UnsupportedStreamsKafkaAttributeException("Attribute called message or described by the \"messageAttribute\" parameter is REQUIRED.");
		}		
	}

	public void initialize(OperatorContext context) throws Exception {
		super.initialize(context);

		getKafkaProperties(context);

	}

	/*
	 * Order of precedence for properties that get used (when duplicate property values provided):
	 * 1. PropertyProvider props overwrite
	 * 2. Properties parameter properties which overwrite. 
	 * 3. Config file properties. 
	 */
	protected void getKafkaProperties(OperatorContext context)
			throws IOException, FileNotFoundException, UnsupportedStreamsKafkaConfigurationException {
		String propFile = getPropertiesFile();
		
		// Lowest priority properties file first
		if (propFile != null) {
			finalProperties.load(new FileReader(propFile));
		}
		
		// Next add properties that were input as parameters
		finalProperties.putAll(properties);
		
		
		// Highest priority PropertyProvider last
		PropertyProvider propertyProvider = null;
		if(getAppConfigName() != null) {
			propertyProvider = new PropertyProvider(context.getPE(), getAppConfigName());
			
			appConfigProperties = getCurrentAppConfigProperties(propertyProvider);
		}	
		finalProperties.putAll(appConfigProperties);
		
		finalProperties = transformTrustStoreProperty(finalProperties);
		
		
		String jaasFile = getJaasFile();
		if(jaasFile != null) {
			System.setProperty("java.security.auth.login.config", jaasFile);
			// If we are not setting the java.security.auth.login.config
			// we must refresh the Configuration to load the security change
			Configuration config = Configuration.getConfiguration();
			config.refresh();
		}
		
		
		if (finalProperties == null || finalProperties.isEmpty())
			throw new UnsupportedStreamsKafkaConfigurationException(
					"Kafka connection properties must be specified.");
	}

	/*
	 * If appConfigPropName not specified, put all properties
	 * Else, just put the specified properties. 
	 */
	private Properties getCurrentAppConfigProperties(PropertyProvider propertyProvider) {
		Properties localAppConfigProps = new Properties();
		if (appConfigPropName.isEmpty()){
			// Then we want to add all properties from appConfig
			trace.log(TraceLevel.INFO, "Adding all properties from appConfig (appConfigPropertyName not specified): " + getAppConfigName());
			localAppConfigProps.putAll(propertyProvider.getAllProperties());
		} else {
			for (String propName : appConfigPropName){
				localAppConfigProps.put(propName, propertyProvider.getProperty(propName));
			}
		}
		
		return localAppConfigProps;
	}
	
	protected boolean newPropertiesExist(OperatorContext context) {
		PropertyProvider propertyProvider = null;
		boolean newProperties = false;
		if(getAppConfigName() != null) {
			propertyProvider = new PropertyProvider(context.getPE(), getAppConfigName());
			
			Properties currentAppConfigProperties = getCurrentAppConfigProperties(propertyProvider);
			
			if(!currentAppConfigProperties.equals(appConfigProperties)){
				newProperties = true;
			}
		}	
		
		return newProperties;
	}

	private Properties transformTrustStoreProperty(Properties props) {
		final String trustorePropertyName = "ssl.truststore.location";
		final String trustorePasswordPropertyName = "ssl.truststore.password";
		final String securityProtocolPropertyName = "security.protocol";
		String trustStoreFile = props.getProperty(trustorePropertyName);
		String securityProtocol = props.getProperty(securityProtocolPropertyName);
		String trustStorePassword = props.getProperty(trustorePasswordPropertyName);
		if (trustStoreFile != null){
			trustStoreFile = getAbsoluteFilePath(trustStoreFile);
			props.setProperty(trustorePropertyName, trustStoreFile);
			trace.log(TraceLevel.INFO, "TrustStore location set to " + trustStoreFile);
		} else if (securityProtocol != null && (securityProtocol.equalsIgnoreCase("SSL")
												|| securityProtocol.equalsIgnoreCase("SASL_SSL"))){
			Map<String, String> env = System.getenv();
			//get java default truststore
			trustStoreFile = env.get("STREAMS_INSTALL") + "/java/jre/lib/security/cacerts";
			props.setProperty(trustorePropertyName, trustStoreFile);
			if (trustStorePassword == null)
				props.setProperty(trustorePasswordPropertyName, "changeit");
			trace.log(TraceLevel.WARN, "Automatically setting to default Java trust store.");
		}
		return props;
	}

	public void initSchema(StreamSchema ss) throws Exception {
		trace.log(TraceLevel.INFO, "Connection properties: " + finalProperties);

		Set<MetaType> supportedTypes = new HashSet<MetaType>();
		supportedTypes.add(MetaType.RSTRING);
		supportedTypes.add(MetaType.USTRING);
		supportedTypes.add(MetaType.BLOB);

		keyAH.initialize(ss, false, supportedTypes);
		messageAH.initialize(ss, true, supportedTypes);

		// blobs are not supported for topics
		supportedTypes.remove(MetaType.BLOB);
		topicAH.initialize(ss, false, supportedTypes);
		
	}

	@Parameter(cardinality = -1, optional = true, description = "Specify a Kafka property \\\"key=value\\\" form. "
			+ "This will override any property specified in the properties file. "
			+ "The hierarchy of properties goes: properties from appConfig beat out kafkaProperty parameter properties, which beat out properties from the propertiesFile. ")
	public void setKafkaProperty(List<String> values) throws UnsupportedStreamsKafkaConfigurationException {
		for (String value : values) {
			int idx = value.indexOf("=");
			if (idx == -1)
				throw new UnsupportedStreamsKafkaConfigurationException("Invalid property: " + value
						+ ", not in the key=value format");
			String name = value.substring(0, idx);
			String v = value.substring(idx + 1, value.length());
			properties.setProperty(name, v);
		}
	}

	@Parameter(optional = true, description = "Properties file containing kafka properties. "
			+ "Properties file is recommended to be stored in the etc directory.  "
			+ "If a relative path is specified, the path is relative to the application directory. "
			+ "The hierarchy of properties goes: properties from appConfig beat out kafkaProperty parameter properties, which beat out properties from the propertiesFile. ")
	public void setPropertiesFile(String value) {
		this.propertiesFile = value;
	}

	public String getPropertiesFile() {
		trace.log(TraceLevel.TRACE, "Properties file: " + propertiesFile);
    	if (propertiesFile == null) return null;
    	propertiesFile = getAbsoluteFilePath(propertiesFile);
		return propertiesFile;
	}        

	@Parameter(optional = true, description = "This parameter specifies the name of application configuration that stores client properties, "
			+ "the property specified via application configuration is overridden by the properties file and kafkaProperty parameter. "
			+ "The hierarchy of properties goes: properties from appConfig beat out kafkaProperty parameter properties, which beat out properties from the propertiesFile. ")
	public void setAppConfigName(String appConfigName) {
		this.appConfigName = appConfigName;
	}
	
	public String getAppConfigName() {
		return appConfigName;
	}
	
	@Parameter(optional = true, description = "List of Kafka properties to retrieve from application configuration. "
			+ "The property name in the application configuration must the same as the Kafka property name. "
			+ "You may also supply jaasFile as a property name to act as the jaasFile parameter value.")
	public void setAppConfigPropertyName(List<String> propNames) {
		if (propNames != null)
			this.appConfigPropName.addAll(propNames);
	}

	@Parameter(optional = true, description = "This parameter specifies the property name of the jaasFile location in the application configuration. "
			+ "The default name is jaasFile. ")
	public void setJaasFilePropName(String jaasFilePropName) {
		this.jaasFilePropName  = jaasFilePropName;
	}
	
	@Parameter(optional = true, description = "Location of the jaas file to be used for SASL connections. "
			+ "Jaas file is recommended to be stored in the etc directory.  "
			+ "If a relative path is specified, the path is relative to the application directory."
			+ "This sets the system property java.security.auth.login.config. This can also be set using the appConfig by "
			+ "specifying jaasFile=<jaas.conf location>.")
	public void setJaasFile(String value) {
		jaasFile = value;
	}

	/*
	 * Check if jaasFile was specified by the jaasFile parameter.
	 * If not, check if it's in our properties list. 
	 */
	public String getJaasFile() {
		
		if (finalProperties.containsKey(jaasFilePropName)){
			jaasFile = finalProperties.getProperty(jaasFilePropName);
			trace.log(TraceLevel.INFO, "Found jaasFile in properties!");
		}
		
		trace.log(TraceLevel.INFO, "Jaas file: " + jaasFile);		
		if (jaasFile == null) return null;
		jaasFile = getAbsoluteFilePath(jaasFile);
		return jaasFile;
	}
	
	public String getAbsoluteFilePath(String filePath){
		File file = new File(filePath);
		
		// if the properties file is relative, the path is relative to the application directory
		if (!file.isAbsolute())
		{
			filePath = getOperatorContext().getPE().getApplicationDirectory().getAbsolutePath() + "/" +  filePath;
		}
		return filePath;
	}

	@Parameter(optional = true, description = "Name of the attribute for the message. If this parameter is not specified, then by default the operator will look for an attribute named \\\"message\\\". If the \\\"message\\\" attribute is not found, a runtime error will be returned.")
	public void setMessageAttribute(String value) {
		messageAH.setName(value);
	}

	@Parameter(optional = true, description = "Name of the attribute for the key. Default is \\\"key\\\".")
	public void setKeyAttribute(String value) {
		keyAH.setName(value);
	}

	@Override
	public void shutdown() throws Exception {

        OperatorContext context = getOperatorContext();
        trace.log(TraceLevel.ALL, "Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
        
        // Must call super.shutdown()
        super.shutdown();
	}
	
	public static final String BASE_DESC = 	"Specify properties as described here: http://kafka.apache.org/documentation.html#configuration. "
			+ "If you are using Java security modules for login/authentication, ensure that they are compatible with IBM Java, as IBM Streams only runs "
			+ "with IBM Java. The SPL Attribute Types supported are rstring, ustring, and blob. The topic must be of type rstring/ustring, while the key and message must "
			+ "be of the same type (rstring, ustring, or blob). " 
			+ "\\n\\n**Kafka 0.9 Server Support**: "
			+ "By default this toolkit builds with Kafka 0.10 client JARs. The Kafka 0.10 client is not compatible with Kafka 0.9 brokers. "
			+ "To use this operator with Kafka 0.9 brokers, you must rebuild with the kafka-0.9 target after cleaning. From the toolkit root directory: "
			+ "\\n\\n ant clean"
			+ "\\n\\n ant kafka-0.9" + 
			"\\n\\n**AppConfig**: "
			+ "You must provide properties for the operator using at least one of the following parameters: kafkaProperty, propertiesFile, or appConfigName. "
			+ "The hierarchy of properties goes: properties from appConfig beat out kafkaProperty parameter properties, which beat out properties from the propertiesFile. ";
}
