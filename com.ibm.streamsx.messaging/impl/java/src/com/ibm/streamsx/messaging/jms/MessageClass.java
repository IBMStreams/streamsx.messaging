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

//enum to specify the supported message class for both the operators

//map=JMS MapMessage (a collection of name/type/value triplets)
//stream=JMS StreamMessage (an ordered list of type/value pairs)
//bytes=JMS BytesMessage (a stream of binary data)
//xml= JMS TextMessage containing a  xml message
//wbe= JMS TextMessage (contains an XML document in the WebSphere Business
// Events event packet format)
//wbe22= JMS TextMessage (contains an XML document in the WebSphere 
// Business old  wbe 2.2 format)
//empty=An empty JMS Message

package com.ibm.streamsx.messaging.jms;

enum MessageClass {
	map, stream, bytes, xml, wbe, wbe22, empty;
}
