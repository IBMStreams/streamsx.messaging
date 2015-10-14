/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.messaging.kafka;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.Parameter;

/**
 * Base operator for all the common functions
 * 
 */
@Libraries({ "opt/downloaded/*","opt/*" })
public abstract class KafkaBaseOper extends AbstractOperator {

	protected Properties properties = new Properties(),
			finalProperties = new Properties();
	protected String propertiesFile = null;
	protected AttributeHelper topicAH = new AttributeHelper("topic"),
			keyAH = new AttributeHelper("key"),
			messageAH = new AttributeHelper("message");
	protected List<String> topics = new ArrayList<String>();
	private final Logger trace = Logger.getLogger(KafkaBaseOper.class
			.getCanonicalName());

	public void initialize(OperatorContext context) throws Exception {
		super.initialize(context);

		String propFile = getPropertiesFile();
		if (propFile != null) {
			finalProperties.load(new FileReader(propFile));
		}
		finalProperties.putAll(properties);
		finalProperties = transformTrustStoreProperty(finalProperties);
		
		if (finalProperties == null || finalProperties.isEmpty())
			throw new Exception(
					"Kafka connection properties must be specified.");

	}

	private Properties transformTrustStoreProperty(Properties props) {
		String trustStoreFile = props.getProperty("ssl.truststore.location");
		if (trustStoreFile != null){
			trustStoreFile = getAbsoluteFilePath(trustStoreFile);
			System.out.println("TrustStore location: " + trustStoreFile);
			props.setProperty("ssl.truststore.location", trustStoreFile);
			trace.log(TraceLevel.INFO, "TrustStore location set to " + trustStoreFile);
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
			+ "This will override any property specified in the properties file.")
	public void setKafkaProperty(List<String> values) {
		for (String value : values) {
			int idx = value.indexOf("=");
			if (idx == -1)
				throw new IllegalArgumentException("Invalid property: " + value
						+ ", not in the key=value format");
			String name = value.substring(0, idx);
			String v = value.substring(idx + 1, value.length());
			properties.setProperty(name, v);
		}
	}

	@Parameter(optional = true, description = "Properties file containing kafka properties.  Properties file is recommended to be stored in the etc directory.  If a relative path is specified, the path is relative to the application directory.")
	public void setPropertiesFile(String value) {
		this.propertiesFile = value;
	}

	public String getPropertiesFile() {
		trace.log(TraceLevel.TRACE, "Properties file: " + propertiesFile);
    	if (propertiesFile == null) return null;
    	propertiesFile = getAbsoluteFilePath(propertiesFile);
		return propertiesFile;
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

	@Parameter(optional = true, description = "Name of the attribute for the message. This attribute is required. Default is \\\"message\\\".")
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
}
