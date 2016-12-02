/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.jms;

import java.net.MalformedURLException;

import com.ibm.streams.operator.OperatorContext;

public class JmsClasspathUtil {


	public static void setupClassPaths(OperatorContext context) {
		String AMQ_HOME = System.getenv("STREAMS_MESSAGING_AMQ_HOME"); //$NON-NLS-1$
		if (AMQ_HOME != null) {
			String lib = AMQ_HOME + "/lib/*"; //$NON-NLS-1$
			String libOptional = AMQ_HOME + "/lib/optional/*"; //$NON-NLS-1$
	
			try {
				context.addClassLibraries(new String[] { lib, libOptional });
			} catch (MalformedURLException e) {
	
			}
		}
	
		String WMQ_HOME = System.getenv("STREAMS_MESSAGING_WMQ_HOME"); //$NON-NLS-1$
		if (WMQ_HOME != null) {
			String javaLib = WMQ_HOME + "/java/lib/*"; //$NON-NLS-1$
			try {
				context.addClassLibraries(new String[] { javaLib });
			} catch (MalformedURLException e) {
	
			}
		}
	}

}