package com.ibm.streamsx.messaging.common;

import java.io.IOException;

public class PropertyProvider {
	// provider instance
	private IProvider provider = null;
	
	private PropertyProvider() {
		
	}

	// Create a file based property provider
	public synchronized static PropertyProvider getFilePropertyProvider(String fileName) throws IOException {
		PropertyProvider p = new PropertyProvider();
		p.loadFileProperties(fileName);
		return p;
	}
	
	// Load file based properties file
	private void loadFileProperties(String fileName) throws IOException {
		provider = new FileBasedPropertyProvider(fileName);
	}

	// Retrieve a property value
	public String getProperty(String proptyName) throws IOException {
		provider.refresh();
		return provider.getProperty(proptyName);
	}
}
