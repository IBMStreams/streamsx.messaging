//
// *******************************************************************************
// * Copyright (C)2014, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
//
package com.ibm.streamsx.messaging.kafka;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.types.RString;

//Helper to check if attributes have been specified explicitly
class AttributeHelper {
	private boolean wasSet = false, isAvailable = false;
	private MetaType mType = null;
	private String name = null;
	AttributeHelper(String n) {
		this.name = n;
	}
	
	public boolean isWasSet() {
		return wasSet;
	}

	
	public boolean isAvailable() {
		return isAvailable;
	}

	public MetaType getmType() {
		return mType;
	}

	public String getName() {
		return name;
	}

	public void wasSet(boolean wasSet) {
		this.wasSet = wasSet;
	}

	public void setName(String name) {
		this.name = name;
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
		setType(a.getType().getMetaType());
		isAvailable = true;
	}
	private void setType(MetaType mt) throws Exception {
		this.mType = mt;
		if(mType != Type.MetaType.RSTRING && mType!= Type.MetaType.USTRING){
			throw new Exception("Attribute \"" + name + "\" must be of RSTRING or USTRING type.");
		}
	}
	void setValue(OutputTuple otup, String value) {
		if(!isAvailable) return;
		if(mType == MetaType.USTRING)
			otup.setString(name, value);
		else if(mType == MetaType.RSTRING)
			otup.setObject(name, new RString(value));
	}
	void setValue(OutputTuple otup, byte[] value) {
		if(!isAvailable) return;
		if(mType == MetaType.USTRING)
			otup.setString(name, new String(value));
		else if(mType == MetaType.RSTRING)
			otup.setObject(name, new RString(value));
	}
	String getValue(Tuple tuple) {
		if(!isAvailable) return null;
		if(mType == MetaType.USTRING)
			return tuple.getString(name);
		return tuple.getObject(name).toString();
	}
}
