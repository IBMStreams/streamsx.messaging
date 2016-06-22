/*******************************************************************************
 * Copyright (C) 2016, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.messaging.common;

import java.util.Map;

import com.ibm.streams.operator.ProcessingElement;

// This class provides configuration data stored in PE
public class PropertyProvider {
	
	private ProcessingElement pe;
	private String configurationName;
	private Map<String,String> configuration;

	public PropertyProvider(ProcessingElement pe, String configurationName) {
		this.pe = pe;
		this.configurationName = configurationName;
		this.loadConfiguration();
		
		if(configuration.isEmpty()) {
			throw new IllegalArgumentException("Application Configuration " + configurationName + " is not found or empty" );
		}
	}
	
	// get a property value by name
	// reload configuration each time to get latest property value
	public String getProperty(String name) {
		this.loadConfiguration();
		return configuration.get(name);
	}
	
	// check if the property provider contains a certain property
	// reload configuration each time to get latest property value
	public boolean contains(String name) {
		this.loadConfiguration();
		return configuration.containsKey(name);
	}
	
	// get a all properties
	// reload configuration each time to get latest property value
	public Map<String, String> getAllProperties() {
		this.loadConfiguration();
		return configuration;
	}
	
	private void loadConfiguration() {
		configuration = pe.getApplicationConfiguration(configurationName);
	}
}
