/* begin_generated_IBM_copyright_prolog                             */
/*                                                                  */
/* This is an automatically generated copyright prolog.             */
/* After initializing,  DO NOT MODIFY OR MOVE                       */
/* **************************************************************** */
/* IBM Confidential                                                 */
/* OCO Source Materials                                             */
/* 5724-Y95                                                         */
/* (C) Copyright IBM Corp.  2013, 2013                              */
/* The source code for this program is not published or otherwise   */
/* divested of its trade secrets, irrespective of what has          */
/* been deposited with the U.S. Copyright Office.                   */
/*                                                                  */
/* end_generated_IBM_copyright_prolog                               */
package com.ibm.streamsx.messaging.jms;

//Class that holds the elements(name, type and length) of an attribute in native schema element.
public class NativeSchema {
	/* begin_generated_IBM_copyright_code */
	public static final String IBM_COPYRIGHT = " Licensed Materials-Property of IBM                              " + //$NON-NLS-1$ 
			" 5724-Y95                                                        "
			+ //$NON-NLS-1$ 
			" (C) Copyright IBM Corp.  2013, 2013    All Rights Reserved.     "
			+ //$NON-NLS-1$ 
			" US Government Users Restricted Rights - Use, duplication or     "
			+ //$NON-NLS-1$ 
			" disclosure restricted by GSA ADP Schedule Contract with         "
			+ //$NON-NLS-1$ 
			" IBM Corp.                                                       "
			+ //$NON-NLS-1$ 
			"                                                                 "; //$NON-NLS-1$ 
	/* end_generated_IBM_copyright_code */
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
