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
		
		if (!props.containsKey("key.deserializer")){
			if(keyAH.isString()){
				props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
				trace.log(TraceLevel.INFO, "Adding unspecified property key.serializer=org.apache.kafka.common.serialization.StringDeserializer" );
			}
			else{
				props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
				trace.log(TraceLevel.INFO, "Adding unspecified property key.serializer=org.apache.kafka.common.serialization.ByteArrayDeserializer" );
			}
		}
		
		if (!props.containsKey("value.deserializer")){
			if(messageAH.isString()){
				props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
				trace.log(TraceLevel.INFO, "Adding unspecified property value.serializer=org.apache.kafka.common.serialization.StringDeserializer" );
			}
			else{
				props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
				trace.log(TraceLevel.INFO, "Adding unspecified property value.serializer=org.apache.kafka.common.serialization.ByteArrayDeserializer" );
			}
		}
		
		return props;
	}
	
	public static Properties setDefaultSerializers(AttributeHelper keyAH, AttributeHelper messageAH, Properties props) {
		final Logger trace = Logger.getLogger(KafkaConfigUtilities.class
				.getCanonicalName());
		
		if (!props.containsKey("key.serializer")){
			if(keyAH.isString()){
				props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
				trace.log(TraceLevel.INFO, "Adding unspecified property key.serializer=org.apache.kafka.common.serialization.StringSerializer" );
			}
			else{
				props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
				trace.log(TraceLevel.INFO, "Adding unspecified property key.serializer=org.apache.kafka.common.serialization.ByteArraySerializer" );
			}
		}
		
		if (!props.containsKey("value.serializer")){
			if(messageAH.isString()){
				props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
				trace.log(TraceLevel.INFO, "Adding unspecified property value.serializer=org.apache.kafka.common.serialization.StringSerializer" );
			}
			else{
				props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
				trace.log(TraceLevel.INFO, "Adding unspecified property value.serializer=org.apache.kafka.common.serialization.ByteArraySerializer" );
			}
		}
		
		return props;
	}
	
	public static String getStringProperty(String propName, Properties props) {
		String propVal = props.getProperty(propName); 
		String stringProp = "";
		final Logger trace = Logger.getLogger(KafkaConfigUtilities.class
				.getCanonicalName());
		if (propVal != null){
				stringProp = props.getProperty(propName);
		}
		trace.log(TraceLevel.INFO, "Property " + propName + " has a final value of " + stringProp);
		return stringProp;
	}
}
