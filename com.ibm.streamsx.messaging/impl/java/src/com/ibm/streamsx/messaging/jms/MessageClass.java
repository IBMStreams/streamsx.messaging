/*******************************************************************************
 * Copyright (C) 2013, 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.messaging.jms;

/** Enum to specify the supported message class for both the operators
 * map=JMS MapMessage (a collection of name/type/value triplets)
 * stream=JMS StreamMessage (an ordered list of type/value pairs)
 * bytes=JMS BytesMessage (a stream of binary data)
 * xml= JMS TextMessage containing a  xml message
 * wbe= JMS TextMessage (contains an XML document in the WebSphere Business
 * Events event packet format)
 * wbe22= JMS TextMessage (contains an XML document in the WebSphere 
 * Business old  wbe 2.2 format)
 * empty=An empty JMS Message
*/
enum MessageClass {
	map, stream, bytes, xml, wbe, wbe22, empty;
}
