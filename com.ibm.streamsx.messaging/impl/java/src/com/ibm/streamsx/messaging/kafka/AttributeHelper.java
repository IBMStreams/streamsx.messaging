//
// *******************************************************************************
// * Copyright (C)2014, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
//
package com.ibm.streamsx.messaging.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Set;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.types.Blob;
import com.ibm.streams.operator.types.ValueFactory;

//Helper to check if attributes have been specified explicitly
class AttributeHelper {
	static final Charset CS = Charset.forName("UTF-8"); 
	private boolean wasSet = false, isAvailable = false;
	private MetaType mType = null;
	private String name = null;
	private boolean isString = false;
	
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

	void initialize(StreamSchema ss, boolean required, Set<MetaType> supportedTypes) throws Exception {
		Attribute a = ss.getAttribute(name);
		if(a == null) {
			if(wasSet)
				throw new IllegalArgumentException("Attribute \"" + name + "\" not available.");
			if(required)
				throw new IllegalArgumentException("Attribute not found for \"" + name + "\".");
			return;
		}
		this.mType = a.getType().getMetaType();
		isString = mType == MetaType.RSTRING || mType == MetaType.USTRING;
		
		if(!supportedTypes.contains(mType)){
			throw new Exception("Attribute \"" + name + "\" must be one of:  " + supportedTypes);
		}
		isAvailable = true;
	}
	boolean isString() {
		return isString;
	}
	
	void setValue(OutputTuple otup, String value) {
		if(!isAvailable) return;
		if(isString) 
			otup.setString(name, value);
		else 
			otup.setBlob(name, ValueFactory.newBlob(value.getBytes(CS)));
	}
	void setValue(OutputTuple otup, byte[] value) {
		if(!isAvailable) return;
		if(isString) 
			otup.setString(name, new String(value, CS));
		else 
			otup.setBlob(name, ValueFactory.newBlob(value));
	}
	String getString(Tuple tuple) throws IOException {
		if(!isAvailable) return null;
		if(isString)
			return tuple.getString(name);
        return new String(getBytesFromBlob(tuple, name));
	}
	byte[] getBytes(Tuple tuple) throws IOException {
		if(!isAvailable) return null;
		if(isString)
			return tuple.getString(name).getBytes(CS);
		return getBytesFromBlob(tuple, name);
	}
	private static byte[] getBytesFromBlob(Tuple tuple, String name) throws IOException {
		Blob blockMsg = tuple.getBlob(name);
        InputStream inputStream = blockMsg.getInputStream();
        int length = (int) blockMsg.getLength();
        byte[] byteArray = new byte[length];
        inputStream.read(byteArray, 0, length);
        return byteArray;
	}
}
