/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.jms;

import java.net.MalformedURLException;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.logging.TraceLevel;

public class JmsClasspathUtil {

	private static final String CLASS_NAME = "com.ibm.streamsx.messaging.jms.JmsClasspathUtil"; //$NON-NLS-1$
	private static final Logger tracer = Logger.getLogger(CLASS_NAME);


	public static void setupClassPaths(OperatorContext context) {
		
		tracer.log(TraceLevel.TRACE, "Setting up classpath"); //$NON-NLS-1$
		
		boolean classpathSet = false;

		
		// Dump the provided environment
		Map<String,String>	sysEnvMap	= System.getenv();
		Set<String>			sysEnvKeys	= sysEnvMap.keySet();
		tracer.log(TraceLevel.TRACE, "------------------------------------------------------------------------------------"); //$NON-NLS-1$
		tracer.log(TraceLevel.TRACE, "--- System Environment used during initialization"); //$NON-NLS-1$
		for( String key : sysEnvKeys) {
			tracer.log(TraceLevel.TRACE, key + " = " + System.getenv(key)); //$NON-NLS-1$
		}
		tracer.log(TraceLevel.TRACE, "------------------------------------------------------------------------------------"); //$NON-NLS-1$
		
		
		String AMQ_HOME = System.getenv("STREAMS_MESSAGING_AMQ_HOME"); //$NON-NLS-1$
		if (AMQ_HOME != null) {
			
			tracer.log(TraceLevel.TRACE, "Apache Active MQ classpath!"); //$NON-NLS-1$

			String lib = AMQ_HOME + "/lib/*"; //$NON-NLS-1$
			String libOptional = AMQ_HOME + "/lib/optional/*"; //$NON-NLS-1$
	
			try {
				tracer.log(TraceLevel.TRACE, "Adding class libs to context"); //$NON-NLS-1$
				context.addClassLibraries(new String[] { lib, libOptional });
			}
			catch (MalformedURLException e) {
				tracer.log(TraceLevel.ERROR, "Failed to add class libs to context: " + e.getMessage()); //$NON-NLS-1$
			}
			classpathSet = true;
		}
	
		String WMQ_HOME = System.getenv("STREAMS_MESSAGING_WMQ_HOME"); //$NON-NLS-1$
		if (WMQ_HOME != null) {
			
			tracer.log(TraceLevel.TRACE, "IBM Websphere MQ classpath!"); //$NON-NLS-1$

			String javaLib = WMQ_HOME + "/java/lib/*"; //$NON-NLS-1$
			try {
				tracer.log(TraceLevel.TRACE, "Adding class libs to context"); //$NON-NLS-1$
				context.addClassLibraries(new String[] { javaLib });
			}
			catch (MalformedURLException e) {
				tracer.log(TraceLevel.ERROR, "Failed to add class libs to context: " + e.getMessage()); //$NON-NLS-1$
			}
			classpathSet = true;
		}
		
		if(	classpathSet != true ) {
			tracer.log(TraceLevel.ERROR, "No classpath has been set!"); //$NON-NLS-1$
		}
		
		tracer.log(TraceLevel.TRACE, "Finished setting up classpath!"); //$NON-NLS-1$

	}

}