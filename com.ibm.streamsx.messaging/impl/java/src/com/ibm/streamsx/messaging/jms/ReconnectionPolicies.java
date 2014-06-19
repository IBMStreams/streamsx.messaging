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

//enum to define the reconnection policies
//supported for both the operators in case
//of both initial or transient connection
//failures.

//The valid values are 
//BoundedRetry=Bounded number of re-tries
//NoRetry=No Retry
//InfiniteRetry= Infinite number of retry
enum ReconnectionPolicies {
	BoundedRetry, NoRetry, InfiniteRetry;
}
