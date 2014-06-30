/*******************************************************************************
 * Copyright (C) 2013, 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.jms;

// This class is used for throwing exceptions
// which comes during parsing of connections.xml

class ParseConnectionDocumentException extends Exception {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	ParseConnectionDocumentException(String message) {
		super(message);
	}
}
