/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.messaging.kafka;

import java.nio.charset.Charset;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;

//Helper to check if attributes have been specified explicitly
class AttributeHelper {
	
	private boolean wasSet = false, isAvailable = false;
	private MetaType mType = null;
	private String name = null;
	AttributeHelper(String n) {
		this.name = n;
	}
	
	boolean isWasSet() {
		return wasSet;
	}
	
	boolean isAvailable() {
		return isAvailable;
	}

	String getName() {
		return name;
	}

	void setName(String name) {
		this.name = name;
		wasSet = true;
	}

	void initialize(StreamSchema ss, boolean required) throws Exception {
		Attribute a = ss.getAttribute(name);
		if(a == null) {
			if(wasSet)
				throw new IllegalArgumentException("Attribute \"" + name + "\" not available.");
			if(required)
				throw new IllegalArgumentException("Attribute not found for \"" + name + "\".");
			return;
		}
		this.mType = a.getType().getMetaType();
		if(mType != Type.MetaType.RSTRING && mType!= Type.MetaType.USTRING){
			throw new Exception("Attribute \"" + name + "\" must be of RSTRING or USTRING type.");
		}
		isAvailable = true;
	}
	
	void setValue(OutputTuple otup, String value) {
		if(!isAvailable) return;
		otup.setString(name, value);
	}
	void setValue(OutputTuple otup, byte[] value) {
		if(!isAvailable) return;
		otup.setString(name, new String(value));
	}
	String getString(Tuple tuple) {
		if(!isAvailable) return null;
		return tuple.getString(name);
	}
	byte[] getBytes(Tuple tuple) {
		if(!isAvailable) return null;
		return tuple.getString(name).getBytes(Charset.forName("UTF-8"));
	}
}
