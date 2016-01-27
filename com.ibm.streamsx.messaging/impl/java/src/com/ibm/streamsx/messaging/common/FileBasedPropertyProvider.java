package com.ibm.streamsx.messaging.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

// File based properties provider class that loads properties from java standard property file
class FileBasedPropertyProvider implements IProvider {

	private Properties prop;
	private String fileName;

	// initialize a file based provider that loads from file
	FileBasedPropertyProvider(String fileName) throws IOException {
		this.prop = new Properties();
		this.fileName = fileName;
		loadProperties();
	}

	// get a property value from file based provider
	@Override
	public String getProperty(String name) {
		return this.prop.getProperty(name);
	}

	@Override
	public void refresh() throws IOException {
		loadProperties();
	}

	private void loadProperties() throws IOException {
		InputStream input = null;
		try {
			input = new FileInputStream(this.fileName);
			this.prop.load(input);
		} catch (IOException e) {
			throw e;
		} finally {
			if (input != null) {
				input.close();
			}
		}
	}
}
