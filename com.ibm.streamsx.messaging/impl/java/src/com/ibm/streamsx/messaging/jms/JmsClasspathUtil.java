/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.jms;

import java.net.MalformedURLException;

import com.ibm.streams.operator.OperatorContext;

public class JmsClasspathUtil {


	public static void setupClassPaths(OperatorContext context) {
		String AMQ_HOME = System.getenv("STREAMS_MESSAGING_AMQ_HOME");
		if (AMQ_HOME != null) {
			String lib = AMQ_HOME + "/lib/*";
			String libOptional = AMQ_HOME + "/lib/optional/*";
	
			try {
				context.addClassLibraries(new String[] { lib, libOptional });
			} catch (MalformedURLException e) {
	
			}
		}
	
		String WMQ_HOME = System.getenv("STREAMS_MESSAGING_WMQ_HOME");
		if (WMQ_HOME != null) {
			String javaLib = WMQ_HOME + "/java/lib/*";
			try {
				context.addClassLibraries(new String[] { javaLib });
			} catch (MalformedURLException e) {
	
			}
		}
	}

}