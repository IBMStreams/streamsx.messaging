/*******************************************************************************
 * Copyright (C) 2013, 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

/* C++ methods, common to both XMSSource and XMSSink */
#define MAX_ERROR_MSG_LEN 2500

#include "MessagingResource.h"

private:

// Configuration parameters
xms::String * 	pInitialContext;
xms::String * 	pConnectionFactory;
xms::String * 	pDestination;
xms::String * 	pQueueURI;
xms::String * 	pTopicURI;
xmsINT  		iDeliveryMode;
xms::String * 	pUserID;
xms::String * 	pPassword;
xms::String * 	pNamespaceURI;
xmsMESSAGE_TYPE msgType;

// XMS api objects
xms::ConnectionFactory * pConnFact;
xms::Destination       * pDest;
xms::Connection          conn;
xms::Session             sess;
xms::Destination         dest;
xms::MessageProducer     producer;
xms::MessageConsumer     consumer;
xms::MessageListener   * pListener;

// State
SPL::boolean fatalError;
SPL::boolean connected;

public:

//To sum up the initialization error messages
ostringstream InitializationErrMsg;

public:

/*******************************************************************
 * Finalize: Close connection and delete resources                 *
 *******************************************************************/
void finalizeOperator() {
	SPLAPPTRC(L_DEBUG, "Entry: finalizeOperator", "XMSCommon");

	/*
	 * Close the connection. This will automatically close and delete dependent objects
	 */
	try {
		conn.close();
	} catch (std::exception & ex) {
	}

	// indicate that we don't have a Connection any more
	connected = false; 

	/*
	 * Clean up any allocated resources
	 */
	delete pInitialContext;
	delete pConnectionFactory;
	delete pDestination;
	delete pQueueURI;
	delete pTopicURI;
	delete pUserID;
	delete pPassword;
	delete pNamespaceURI;
	delete pConnFact;
	delete pDest;
	delete pListener;

	SPLAPPTRC(L_DEBUG, "Exit: finalizeOperator", "XMSCommon");
}

private:

/*******************************************************************
 * Create the administered objects needed to access XMS            *
 *******************************************************************/
xmsINT createAdminObjects() {
	SPLAPPTRC(L_DEBUG, "Entry: createAdminObjects", "XMSCommon");

	//Clear the InitializationErrMsg
	ostringstream InitializationErrMsg;

	xmsINT nRC = XMS_OK;

	try {
		SPLAPPTRC(L_TRACE, "About to create Initial Context object", "XMSCommon");
		// Create the context used for looking up ConnectionFactory and Destination
		xms::InitialContext context;

		if (pInitialContext == NULL) {
			SPLAPPLOG(L_ERROR, MSGTK_INITIALCONTEXT_NON_EXISTENT, "XMSCommon");
			if (isErrorPortSpecified==true)
			{
				//Append to the InitializationErrMsg
				InitializationErrMsg<<MSGTK_INITIALCONTEXT_NON_EXISTENT;
			}
			nRC = XMS_E_BAD_PARAMETER;
		}
		else {
			SPLAPPTRC(L_TRACE, "Initial Context = " << *pInitialContext, "XMSCommon");
			context.create(*pInitialContext);
			try {
				SPLAPPTRC(L_TRACE, "About to look up the Connection Factory " << *pConnectionFactory, "XMSCommon");
				// Lookup & create the connection factory
				xms::PropertyContext * pPC = context.lookup(*pConnectionFactory);
				if (pPC == NULL) {
					SPLAPPLOG(L_ERROR, MSGTK_CONFAC_LOOKUP_FAILURE, "XMSCommon");
					if (isErrorPortSpecified==true)
					{
						//Append to the InitializationErrMsg
						InitializationErrMsg<<MSGTK_CONFAC_LOOKUP_FAILURE;
					}
					nRC = XMS_E_BAD_PARAMETER;
				}
				else{
					SPLAPPTRC(L_TRACE, "Looked up Connection Factory " << *pConnectionFactory, "XMSCommon");
					pConnFact = dynamic_cast<xms::ConnectionFactory *> (pPC);
				}	

				// Lookup & create the destination, if we are using an administered object
				if (pDestination != NULL) {
					SPLAPPTRC(L_TRACE, "About to look up the Destination " << *pDestination, "XMSCommon");
					pPC = context.lookup(*pDestination);
					if (pPC == NULL) {
						SPLAPPLOG(L_ERROR, MSGTK_DEST_LOOKUP_FAILURE, "XMSCommon");
						if (isErrorPortSpecified==true)
						{
							//Append to the InitializationErrMsg
							InitializationErrMsg<<MSGTK_DEST_LOOKUP_FAILURE;
						}
						nRC = XMS_E_BAD_PARAMETER;
					}
					else
						SPLAPPTRC(L_TRACE, "Looked up Destination " << *pDestination, "XMSCommon");
					pDest = dynamic_cast<xms::Destination *> (pPC);
				}
			} catch (xms::Exception & ex) {
				// Unable to lookup connection factory or destination.
				SPLAPPLOG(L_ERROR, MSGTK_CONFAC_DEST_LOOKUP_FAILURE, "XMSCommon");
				if (isErrorPortSpecified==true)
				{
					//Append to the InitializationErrMsg
					InitializationErrMsg<<MSGTK_CONFAC_DEST_LOOKUP_FAILURE;
				}
				processException(ex);
				nRC = ex.getErrorCode();
			}
		}
	} catch (xms::Exception & ex) {
		// Unable to create initial context.
		SPL::rstring logmsg = MSGTK_INITIALCONTEXT_CREATION_ERROR(ex.getErrorString().c_str());
		SPLAPPLOG(L_ERROR, logmsg, "XMSCommon");
		if (isErrorPortSpecified==true)
		{
			//Append to the InitializationErrMsg
			InitializationErrMsg<<logmsg;
		}
		processException(ex);
		nRC = ex.getErrorCode();
	} catch (std::exception & ex) {
		// Unable to create initial context.
		SPLAPPLOG(L_ERROR, MSGTK_ADMINISTERED_OBJECT_ERROR, "XMSCommon");
		if (isErrorPortSpecified==true)
		{
			//Append to the InitializationErrMsg
			InitializationErrMsg<<MSGTK_ADMINISTERED_OBJECT_ERROR;
		}
		nRC = -1;
	}
	SPLAPPTRC(L_DEBUG, "Exit: createAdminObjects " << nRC, "XMSCommon");
	return nRC;
}

/*******************************************************************
 * Create XMS objects needed to send messages via XMS              *
 * This assumes that the Administered Objects have been created    *
 *******************************************************************/

#define PRODUCER 1
#define CONSUMER 2
#define OPERATOR_SHUTDOWN_IN_PROGRESS 5555

xmsINT createXMS(const xmsINT producerOrConsumer,const xmsINT reconnectionPolicy, const xmsINT reconnectionBound, const xmsFLOAT period ) {

	SPLAPPTRC(L_DEBUG, "Entry: createXMS", "XMSCommon");

	xmsINT nRC = XMS_OK;

	try {
		SPLAPPLOG(L_INFO, MSGTK_XMS_CONNECT, "XMSCommon");

		//set the reconnectionAttemptDelay to period
		float reconnectionAttemptDelay = period;


		nConnectionAttempts++;
		updatePerformanceCounters();

		SPLAPPTRC(L_INFO, "createXMS - trying to connect", "XMSCommon");
		if ((nRC=connect())!=XMS_OK)
		{
			SPLAPPTRC(L_WARN, "createXMS - 1. connect failed. rc = " << nRC << ", reconnectionPolicy = " << reconnectionPolicy, "XMSCommon");

			//Get the reconnectionPolicy value, 1=Bounded retry, 2= NoRetry, 3= InfiniteRetry

			//Check if ReconnectionPolicy is noRetry, then abort
			if(reconnectionPolicy==2){
				SPLAPPLOG(L_ERROR, MSGTK_CONNECTION_FAILURE_NORETRY,"XMSCommon");
				if (isErrorPortSpecified==true)
				{
					InitializationErrMsg<<MSGTK_CONNECTION_FAILURE_NORETRY;
				}
				throw new xms::Exception();
			}

			//Check if ReconnectionPolicy is BoundedRetry, then try once in interval defined by period till the reconnectionBound
			//If no ReconnectionPolicy is mentioned then also we have a default value of reconnectionBound and period
			else if(reconnectionPolicy==1){
				//try for reconnectionBound times
				getPE().blockUntilShutdownRequest((double)reconnectionAttemptDelay);
				if (getPE().getShutdownRequested()) {
					return OPERATOR_SHUTDOWN_IN_PROGRESS;
				}
				xmsINT counter=1;
				nConnectionAttempts++;
				updatePerformanceCounters();
				if(counter<reconnectionBound) {
					SPLAPPTRC(L_INFO, "createXMS - trying to connect", "XMSCommon");
					while (  (nRC=connect())!=XMS_OK){
						counter++;
						nConnectionAttempts++;
						SPLAPPTRC(L_WARN, "createXMS - " << counter << "/" << reconnectionBound << " connect failed. rc = " << nRC, "XMSCommon");
						updatePerformanceCounters();
						SPL::rstring logmsg = MSGTK_CONNECTION_FAILURE_BOUNDEDRETRY(reconnectionAttemptDelay,counter);
						SPLAPPLOG(L_INFO, logmsg, "XMSCommon");
						if(counter >= reconnectionBound) {
							break;
						}
						getPE().blockUntilShutdownRequest((double)reconnectionAttemptDelay);
						if (getPE().getShutdownRequested()) {
							return OPERATOR_SHUTDOWN_IN_PROGRESS;
						}
						SPLAPPTRC(L_INFO, "createXMS - trying to connect", "XMSCommon");
					}
				}


				if (counter==reconnectionBound){
					SPLAPPTRC(L_ERROR, "createXMS - #conectionAttempts exceeds reconnection bound (" << reconnectionBound << ")", "XMSCommon");
					// Bounded number of tries has exceeded
					throw new xms::Exception();
				}

				else {
					//We have got a successful connection
				}

			}

			//Check if ReconnectionPolicy is infiniteRetry, then try once in interval defined by period
			else if(reconnectionPolicy==3){
				//infinitely reconnect
				getPE().blockUntilShutdownRequest((double)reconnectionAttemptDelay);
				if (getPE().getShutdownRequested()) {
					return OPERATOR_SHUTDOWN_IN_PROGRESS;
				}
				xmsINT counter=1;
				nConnectionAttempts++;
				updatePerformanceCounters();
				SPLAPPTRC(L_INFO, "createXMS - trying to connect", "XMSCommon");
				while ((nRC=connect())!=XMS_OK){
					++counter;
					SPLAPPTRC(L_WARN, "createXMS - " << counter << ". connect failed. rc = " << nRC, "XMSCommon");
					nConnectionAttempts++;
					updatePerformanceCounters();
					SPL::rstring logmsg = MSGTK_CONNECTION_FAILURE_INFINITERETRY(reconnectionAttemptDelay);
					SPLAPPLOG(L_INFO, logmsg,"XMSCommon");
					getPE().blockUntilShutdownRequest((double)reconnectionAttemptDelay);
					if (getPE().getShutdownRequested()) {
						return OPERATOR_SHUTDOWN_IN_PROGRESS;
					}
					SPLAPPTRC(L_INFO, "createXMS - trying to connect", "XMSCommon");
				}
			}
		}

		SPLAPPTRC(L_INFO, "createXMS - connection successfully created: " << nRC, "XMSCommon");
		SPLAPPLOG(L_INFO, MSGTK_CONNECTION_SUCCESSFUL, "XMSCommon");
		SPLAPPTRC(L_DEBUG, "Now creating other XMS api objects", "XMSCommon");

		try {
			SPLAPPTRC(L_DEBUG, "About to create Session", "XMSCommon");

			// Create the session

			sess = conn.createSession(xmsFALSE, XMSC_AUTO_ACKNOWLEDGE);
			SPLAPPTRC(L_DEBUG, "Session created", "XMSCommon");

			try {
				// Create the destination if not already retrieved from initial context

				if (pDest != NULL)
					dest = *pDest;
				else if (pQueueURI != NULL) {
					SPLAPPTRC(L_DEBUG, "About to create Queue destination", "XMSCommon");
					dest = sess.createQueue(*pQueueURI);
				}
				else if (pTopicURI != NULL) {
					SPLAPPTRC(L_DEBUG, "About to create Topic destination", "XMSCommon");
					dest = sess.createTopic(*pTopicURI);
				}
				else
					throw xms::InvalidDestinationException();

				SPLAPPTRC(L_DEBUG, "Using Destination " << dest.toString(), "XMSCommon");

				if (producerOrConsumer == PRODUCER){
					try {
						SPLAPPTRC(L_DEBUG, "About to create Producer", "XMSCommon");
						// Create the producer
						producer = sess.createProducer(dest);

						SPLAPPTRC(L_DEBUG, "Producer created", "XMSCommon");

						try {
							SPLAPPTRC(L_DEBUG, "About to set Producer's Delivery Mode", "XMSCommon");
							producer.setIntProperty(XMSC_DELIVERY_MODE,
									iDeliveryMode);
						} catch (xms::Exception & ex) {
							// Unable to set the delivery mode
							SPL::rstring logmsg = MSGTK_SET_DELIVERY_MODE_FAILURE(ex.getErrorString().c_str());
							SPLAPPLOG(L_ERROR, logmsg, "XMSCommon");
							if (isErrorPortSpecified==true)
							{
								//Added  to append to the InitializationErrMsg
								InitializationErrMsg<<logmsg;
							}
							processException(ex);
							nRC = ex.getErrorCode();
						}

					} catch (xms::Exception & ex) {
						// Unable to create producer
						SPL::rstring logmsg = MSGTK_CREATE_PRODUCER_EXCEPTION(ex.getErrorString().c_str());
						SPLAPPLOG(L_ERROR, logmsg,"XMSCommon");
						if (isErrorPortSpecified==true)
						{
							//Added  to append to the InitializationErrMsg
							InitializationErrMsg<<logmsg;
						}
						processException(ex);
						nRC = ex.getErrorCode();
					}
				}
				else  {
					try {
						SPLAPPTRC(L_DEBUG, "About to create Consumer", "XMSCommon");
						// Create the consumer
						consumer = sess.createConsumer(dest);

						SPLAPPTRC(L_DEBUG, "Consumer created", "XMSCommon");

					} catch (xms::Exception & ex) {
						// Unable to create consumer
						SPL::rstring logmsg = MSGTK_CREATE_CONSUMER_EXCEPTION(ex.getErrorString().c_str());
						SPLAPPLOG(L_ERROR, logmsg, "XMSCommon");
						if (isErrorPortSpecified==true)
						{
							//Added  to append to the InitializationErrMsg
							InitializationErrMsg<<logmsg;
						}
						processException(ex);
						nRC = ex.getErrorCode();
					}
				}
			} catch (xms::Exception & ex) {
				// Unable to create destination
				SPL::rstring logmsg = MSGTK_CREATE_DESTINATION_EXCEPTION(ex.getErrorString().c_str());
				SPLAPPLOG(L_ERROR, logmsg, "XMSCommon");
				if (isErrorPortSpecified==true)
				{
					//Added  to append to the InitializationErrMsg
					InitializationErrMsg<<logmsg;
				}
				processException(ex);
				nRC = ex.getErrorCode();
			}
		} catch (xms::Exception & ex) {
			// Unable to create the session
			SPL::rstring logmsg = MSGTK_CREATE_SESSION_EXCEPTION(ex.getErrorString().c_str());
			SPLAPPLOG(L_ERROR, logmsg, "XMSCommon");
			if (isErrorPortSpecified==true)
			{
				//Added  to append to the InitializationErrMsg
				InitializationErrMsg<<logmsg;
			}
			processException(ex);
			nRC = ex.getErrorCode();
		}

	} catch (std::exception & ex) {
		// Some other exception
		SPL::rstring logmsg = MSGTK_XMS_API_OBJECT_ERROR(ex.what());
		SPLAPPLOG(L_ERROR, logmsg, "XMSCommon");
		if (isErrorPortSpecified==true)
		{
			//Append to the InitializationErrMsg
			InitializationErrMsg<<logmsg;
		}
		nRC = -1;
	} catch (...) {
		// Unknown other exception
		SPLAPPLOG(L_ERROR, MSGTK_XMS_API_UNKNOWN_EXCEPTION, "XMSCommon");
		if (isErrorPortSpecified==true)
		{
			//Append to the InitializationErrMsg
			InitializationErrMsg<<MSGTK_XMS_API_UNKNOWN_EXCEPTION;
		}
		nRC = -1;
	}
	SPLAPPTRC(L_DEBUG, "Exit: createXMS " << nRC, "XMSCommon");
	return nRC;
}

/*******************************************************************
 * Method to make an XMS connection                                *
 *******************************************************************/
xmsINT connect() {
	xmsINT nRC = XMS_OK;

	try
	{
		if ((pUserID != NULL) && (pPassword != NULL)) {
			SPLAPPTRC(L_DEBUG, "Calling createConnection with userid = " << *pUserID , "XMSCommon");
			conn = pConnFact->createConnection(*pUserID, *pPassword);
		}
		else {
			SPLAPPTRC(L_DEBUG, "Calling createConnection without parameters", "XMSCommon");
			conn = pConnFact->createConnection();
		}
	} catch (xms::Exception & ex) {
		// Unable to create the connection
		SPL::rstring logmsg = MSGTK_CREATE_CONNECTION_EXCEPTION(ex.getErrorString().c_str());
		SPLAPPLOG(L_ERROR, logmsg, "XMSCommon");
		ostringstream ost;
		ex.dump (ost);
		SPLAPPTRC (L_ERROR, "connect(): " << ost.str(), "XMSCommon");
		if (isErrorPortSpecified==true)
		{
			//Append to the InitializationErrMsg
			InitializationErrMsg<<logmsg;
		}
		processException(ex);
		nRC = ex.getErrorCode();
	}

	return nRC;
}

/*******************************************************************
 * Method to Format and output an XMS error                        *
 *                                                                 *
 *******************************************************************/
xmsVOID processException(const xms::Exception  & ex) {
	dumpError(ex.getHandle());

}

/*******************************************************************
 * C++ routine to Format and output an XMS error                     *
 *                                                                 *
 *******************************************************************/
/*
 * Format and output an XMS error
 * PARAMETERS: hError - handle to the XMS error block
 * RETURNS:    xmsVOID
 */

xmsVOID dumpError(xmsHErrorBlock hError)
{
	xmsCHAR        szText[100]  = { '\0'};
	xmsINT         cbTextSize   = sizeof(szText) / sizeof(xmsCHAR);
	xmsCHAR        szData[1000] = { '\0'};
	xmsINT         cbDataSize   = sizeof(szData) / sizeof(xmsCHAR);
	xmsINT         cbActualSize = 0;
	xmsINT         nReason      = XMS_E_NONE;
	xmsJMSEXP_TYPE jmsexception = XMS_X_NO_EXCEPTION;
	xmsCHAR        szErrorMsg[1024];   
	xmsCHAR *      pszExceptionType = NULL;


	if (hError != NULL)
	{
		xmsHErrorBlock xmsLinkedError = (xmsHErrorBlock) XMS_NULL_HANDLE;
		xmsErrorGetJMSException(hError, &jmsexception);
		xmsErrorGetErrorCode(hError, &nReason);
		xmsErrorGetErrorString(hError, szText, cbTextSize, &cbActualSize);
		xmsErrorGetErrorData(hError, szData, cbDataSize, &cbActualSize);

		switch (jmsexception)
		{
		case XMS_X_NO_EXCEPTION:
			pszExceptionType = (xmsCHAR *) "XMS_JMSEXP_TYPE_NONE";
			break;

		case XMS_X_GENERAL_EXCEPTION:
			pszExceptionType = (xmsCHAR *) "XMS_JMSEXP_TYPE_GENERALEXCEPTION";
			break;

		case XMS_X_ILLEGAL_STATE_EXCEPTION:
			pszExceptionType = (xmsCHAR *) "XMS_JMSEXP_TYPE_ILLEGALSTATEEXCEPTION";
			break;

		case XMS_X_INVALID_CLIENTID_EXCEPTION:
			pszExceptionType = (xmsCHAR *) "XMS_JMSEXP_TYPE_INVALIDCLIENTIDEXCEPTION";
			break;

		case XMS_X_INVALID_DESTINATION_EXCEPTION:
			pszExceptionType = (xmsCHAR *) "XMS_JMSEXP_TYPE_INVALIDDESTINATIONEXCEPTION";
			break;

		case XMS_X_INVALID_SELECTOR_EXCEPTION:
			pszExceptionType = (xmsCHAR *) "XMS_JMSEXP_TYPE_INVALIDSELECTOREXCEPTION";
			break;

		case XMS_X_MESSAGE_EOF_EXCEPTION:
			pszExceptionType = (xmsCHAR *) "XMS_JMSEXP_TYPE_MESSAGEEOFEXCEPTION";
			break;

		case XMS_X_MESSAGE_FORMAT_EXCEPTION:
			pszExceptionType = (xmsCHAR *) "XMS_JMSEXP_TYPE_MESSAGEFORMATEXCEPTION";
			break;

		case XMS_X_MESSAGE_NOT_READABLE_EXCEPTION:
			pszExceptionType = (xmsCHAR *) "XMS_JMSEXP_TYPE_MESSAGENOTREADABLEEXCEPTION";
			break;

		case XMS_X_MESSAGE_NOT_WRITEABLE_EXCEPTION:
			pszExceptionType = (xmsCHAR *) "XMS_JMSEXP_TYPE_MESSAGENOTWRITEABLEEXCEPTION";
			break;

		case XMS_X_RESOURCE_ALLOCATION_EXCEPTION:
			pszExceptionType = (xmsCHAR *) "XMS_JMSEXP_TYPE_RESOURCEALLOCATIONEXCEPTION";
			break;

		default:
			pszExceptionType = (xmsCHAR *) "";
			break;
		}


		sprintf(szErrorMsg,"Error Block \n  -> JMSException = %d (%s)\n  -> Error Code   = %d (%s)\n  -> Error Data   = %s\n\n",

				jmsexception, pszExceptionType, nReason, szText, szData);

		SPL::rstring logmsg = MSGTK_XMS_JMS_EXCEPTION(szErrorMsg);

		SPLAPPLOG(L_ERROR, logmsg, "XMSCommon");
		if (isErrorPortSpecified==true)
		{
			//Append to the InitializationErrMsg
			char temp[MAX_ERROR_MSG_LEN];
			strcpy(temp,"");
			sprintf(temp,"Error Block   -> JMSException = %d (%s)  -> Error Code   = %d (%s)  -> Error Data   = %s",jmsexception, pszExceptionType, nReason, szText, szData);
			InitializationErrMsg<<temp;
		}


		/*
		 * Get the next linked error, and act recursively
		 */

		xmsErrorGetLinkedError(hError, &xmsLinkedError);

		dumpError(xmsLinkedError);
	}
}

