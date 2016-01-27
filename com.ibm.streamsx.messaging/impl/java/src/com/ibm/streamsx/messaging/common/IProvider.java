/**
 * 
 */
package com.ibm.streamsx.messaging.common;

import java.io.IOException;

// Interface to be implemented by concrete property provider
interface IProvider {
	// retrieve a property value
	String getProperty(String name);
    // refresh the properties
	void refresh() throws IOException;
}
