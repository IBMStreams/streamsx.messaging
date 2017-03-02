/*******************************************************************************
 * Copyright (C) 2016, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.kafka;

import java.util.Properties;
import java.util.logging.Logger;

import com.ibm.streams.operator.logging.TraceLevel;

public class KafkaConfigUtilities {
	
	public static Properties setDefaultDeserializers(AttributeHelper keyAH, AttributeHelper messageAH, Properties props) {
		final Logger trace = Logger.getLogger(KafkaConfigUtilities.class
				.getCanonicalName());
		
		if (!props.containsKey("key.deserializer")){ //$NON-NLS-1$
			if(keyAH.isString()){
				props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); //$NON-NLS-1$ //$NON-NLS-2$
				trace.log(TraceLevel.INFO, "Adding unspecified property key.serializer=org.apache.kafka.common.serialization.StringDeserializer" ); //$NON-NLS-1$
			}
			else{
				props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer"); //$NON-NLS-1$ //$NON-NLS-2$
				trace.log(TraceLevel.INFO, "Adding unspecified property key.serializer=org.apache.kafka.common.serialization.ByteArrayDeserializer" ); //$NON-NLS-1$
			}
		}
		
		if (!props.containsKey("value.deserializer")){ //$NON-NLS-1$
			if(messageAH.isString()){
				props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); //$NON-NLS-1$ //$NON-NLS-2$
				trace.log(TraceLevel.INFO, "Adding unspecified property value.serializer=org.apache.kafka.common.serialization.StringDeserializer" ); //$NON-NLS-1$
			}
			else{
				props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer"); //$NON-NLS-1$ //$NON-NLS-2$
				trace.log(TraceLevel.INFO, "Adding unspecified property value.serializer=org.apache.kafka.common.serialization.ByteArrayDeserializer" ); //$NON-NLS-1$
			}
		}
		
		return props;
	}
	
	public static Properties setDefaultSerializers(AttributeHelper keyAH, AttributeHelper messageAH, Properties props) {
		final Logger trace = Logger.getLogger(KafkaConfigUtilities.class
				.getCanonicalName());
		
		if (!props.containsKey("key.serializer")){ //$NON-NLS-1$
			if(keyAH.isString()){
				props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //$NON-NLS-1$ //$NON-NLS-2$
				trace.log(TraceLevel.INFO, "Adding unspecified property key.serializer=org.apache.kafka.common.serialization.StringSerializer" ); //$NON-NLS-1$
			}
			else{
				props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer"); //$NON-NLS-1$ //$NON-NLS-2$
				trace.log(TraceLevel.INFO, "Adding unspecified property key.serializer=org.apache.kafka.common.serialization.ByteArraySerializer" ); //$NON-NLS-1$
			}
		}
		
		if (!props.containsKey("value.serializer")){ //$NON-NLS-1$
			if(messageAH.isString()){
				props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //$NON-NLS-1$ //$NON-NLS-2$
				trace.log(TraceLevel.INFO, "Adding unspecified property value.serializer=org.apache.kafka.common.serialization.StringSerializer" ); //$NON-NLS-1$
			}
			else{
				props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer"); //$NON-NLS-1$ //$NON-NLS-2$
				trace.log(TraceLevel.INFO, "Adding unspecified property value.serializer=org.apache.kafka.common.serialization.ByteArraySerializer" ); //$NON-NLS-1$
			}
		}
		
		return props;
	}
	
	public static String getStringProperty(String propName, Properties props) {
		String propVal = props.getProperty(propName); 
		String stringProp = ""; //$NON-NLS-1$
		final Logger trace = Logger.getLogger(KafkaConfigUtilities.class
				.getCanonicalName());
		if (propVal != null){
				stringProp = props.getProperty(propName);
		}
		trace.log(TraceLevel.INFO, "Property " + propName + " has a final value of " + stringProp); //$NON-NLS-1$ //$NON-NLS-2$
		return stringProp;
	}
}
