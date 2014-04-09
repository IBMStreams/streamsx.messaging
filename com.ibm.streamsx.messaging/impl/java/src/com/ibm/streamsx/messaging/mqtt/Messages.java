/*******************************************************************************
 * Licensed Materials - Property of IBM
 * Copyright IBM Corp. 2014
 * US Government Users Restricted Rights - Use, duplication or
 * disclosure restricted by GSA ADP Schedule Contract with
 * IBM Corp.
 *******************************************************************************/
package com.ibm.streamsx.messaging.mqtt;

import java.text.MessageFormat;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

// test revert
public class Messages {
	private static final String BUNDLE_NAME = "com.ibm.streamsx.messaging.mqtt.messages"; //$NON-NLS-1$

	private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle
			.getBundle(BUNDLE_NAME);

	private Messages() {
	}

	public static String getString(String key) {
		try {
			return RESOURCE_BUNDLE.getString(key);
		} catch (MissingResourceException e) {
			return '!' + key + '!';
		}
	}
	
	public static String getString(String key, Object... args)
	{
		try {
			String msg =  RESOURCE_BUNDLE.getString(key);
			return MessageFormat.format(msg, args);
		} catch (MissingResourceException e) {
			return '!' + key + '!';
		}
	}
}
