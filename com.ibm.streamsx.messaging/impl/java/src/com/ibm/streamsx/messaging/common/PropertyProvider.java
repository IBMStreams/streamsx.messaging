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
	
	private void loadConfiguration() {
		configuration = pe.getApplicationConfiguration(configurationName);
	}
}
