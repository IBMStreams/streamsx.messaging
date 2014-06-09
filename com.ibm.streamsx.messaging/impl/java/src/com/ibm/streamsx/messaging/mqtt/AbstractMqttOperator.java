/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.mqtt;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.log4j.LogLevel;
import com.ibm.streams.operator.log4j.LoggerNames;
import com.ibm.streams.operator.log4j.TraceLevel;
import com.ibm.streams.operator.model.Parameter;

public abstract class AbstractMqttOperator extends AbstractOperator {
	
	static Logger TRACE = Logger.getLogger(AbstractMqttOperator.class);
	private static final Logger LOG = Logger.getLogger(LoggerNames.LOG_FACILITY + "." + AbstractMqttOperator.class.getName()); //$NON-NLS-1$

	private String serverUri;
	private String connectionDocument;
	private String connection;

	public AbstractMqttOperator() {
		super();
	}

	@Parameter(name = "serverURI", description = SPLDocConstants.MQTTSRC_PARAM_SERVERIURI_DESC, optional = true)
	public void setServerUri(String serverUri) {
		this.serverUri = serverUri;
	}

	public String getServerUri() {
		return serverUri;
	}

	public String getConnection() {
		return connection;
	}

	@Parameter(name = "connection", description = "Name of the connection specification of the MQTT element in the connection document.", optional = true)
	public void setConnection(String connection) {
		this.connection = connection;
	}

	public String getConnectionDocument() {
		return connectionDocument;
	}

	@Parameter(name = "connectionDocument", description = "Path to connection document.  If unspecified, default to ../etc/connections.xml", optional = true)
	public void setConnectionDocument(String connectionDocument) {
		this.connectionDocument = connectionDocument;
	}

	protected void initializeServerUri() throws Exception {
		
		// if serverUri is null, read connection document
		if (getServerUri()==null)
		{
			ConnectionDocumentHelper helper = new ConnectionDocumentHelper();
			String connDoc = getConnectionDocument();
			
			// if connection document is not specified, default to ../etc/connections.xml
			if (connDoc == null)
			{
				File dataDirectory = getOperatorContext().getPE().getDataDirectory();
				connDoc = dataDirectory.getAbsolutePath() + "/../etc/connections.xml"; //$NON-NLS-1$
			}			
			
			// convert from relative path to absolute path is necessary
			if (!connDoc.startsWith("/")) //$NON-NLS-1$
			{
				File dataDirectory = getOperatorContext().getPE().getDataDirectory();
				connDoc = dataDirectory.getAbsolutePath() + "/" + connDoc; //$NON-NLS-1$
			}
			
			try {
				helper.parseAndValidateConnectionDocument(connDoc);
				ConnectionSpecification connectionSpecification = helper.getConnectionSpecification(getConnection());
				if (connectionSpecification != null)
				{
					setServerUri(connectionSpecification.getServerUri());	
				}
				else
				{
					TRACE.log(TraceLevel.ERROR, Messages.getString("Error_AbstractMqttOperator.3") + getConnection()); //$NON-NLS-1$
					LOG.log(LogLevel.ERROR, Messages.getString("Error_AbstractMqttOperator.3") + getConnection()); //$NON-NLS-1$
					throw new RuntimeException(Messages.getString("Error_AbstractMqttOperator.5")); //$NON-NLS-1$
				}
			} catch (SAXException | IOException e) {
				TRACE.log(LogLevel.ERROR, Messages.getString("Error_AbstractMqttOperator.6")); //$NON-NLS-1$
				throw e;				
			}
		}
	}

	
	protected static void validateNumber(OperatorContextChecker checker,
			String parameterName, long min, long max) {
		try {
			List<String> paramValues = checker.getOperatorContext()
					.getParameterValues(parameterName);
			for (String strVal : paramValues) {
				Long longVal = Long.valueOf(strVal);

				if (longVal.longValue() > max || longVal.longValue() < min) {
					checker.setInvalidContext(
							Messages.getString("Error_AbstractMqttOperator.0"), //$NON-NLS-1$
							new Object[] { parameterName, min, max });
				}
			}
		} catch (NumberFormatException e) {
			checker.setInvalidContext(Messages.getString("Error_AbstractMqttOperator.1"), //$NON-NLS-1$
					new Object[] { parameterName });
		}
	}
}