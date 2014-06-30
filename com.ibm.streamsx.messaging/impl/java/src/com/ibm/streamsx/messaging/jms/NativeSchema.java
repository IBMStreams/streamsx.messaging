/*******************************************************************************
 * Copyright (C) 2013, 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.jms;

//Class that holds the elements(name, type and length) of an attribute in native schema element.
public class NativeSchema {
	
	// variable to hold the name
	private final String name;
	// variable to hold the type
	private final NativeTypes type;
	// variable to hold the length
	private final int length;
	// boolean variable to signify if the native schema element is present in
	// the stream schema or not
	// if present set to true, fasle otherwise.
	private final boolean isPresentInStreamSchema;

	public NativeSchema(String name, NativeTypes type, int length,
			boolean isPresentInStreamSchema) {
		this.name = name;
		this.type = type;
		this.length = length;
		this.isPresentInStreamSchema = isPresentInStreamSchema;
	}

	// getters for the private members
	public String getName() {
		return name;
	}

	public NativeTypes getType() {
		return type;
	}

	public int getLength() {
		return length;
	}

	public boolean getIsPresentInStreamSchema() {
		return isPresentInStreamSchema;
	}
}
