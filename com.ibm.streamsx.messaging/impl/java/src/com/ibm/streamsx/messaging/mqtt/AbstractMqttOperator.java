/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.mqtt;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.log4j.LogLevel;
import com.ibm.streams.operator.log4j.LoggerNames;
import com.ibm.streams.operator.log4j.TraceLevel;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.state.ConsistentRegionContext;

public abstract class AbstractMqttOperator extends AbstractOperator {

	public static final String PARAMNAME_KEY_STORE_PASSWORD = "keyStorePassword"; //$NON-NLS-1$
	public static final String PARAMNAME_KEY_STORE = "keyStore"; //$NON-NLS-1$
	public static final String PARAMNAME_TRUST_STORE_PASSWORD = "trustStorePassword"; //$NON-NLS-1$
	public static final String PARAMNAME_TRUST_STORE = "trustStore"; //$NON-NLS-1$
	public static final String PARAMNAME_CONNDOC = "connectionDocument"; //$NON-NLS-1$
	public static final String PARAMNAME_CONNECTION = "connection"; //$NON-NLS-1$
	public static final String PARAMNAME_SERVER_URI = "serverURI"; //$NON-NLS-1$
	public static final String PARAMNAME_ERROR_OUT_ATTR_NAME = "errorOutAttrName"; //$NON-NLS-1$
	
	public static final String PARAMNAME_CLIENT_ID = "clientID"; //$NON-NLS-1$
	public static final String PARAMNAME_USER_ID = "userID"; //$NON-NLS-1$
	public static final String PARAMNAME_PASSWORD = "password"; //$NON-NLS-1$
	public static final String PARAMNAME_COMMAND_TIMEOUT = "commandTimeout"; //$NON-NLS-1$
	public static final String PARAMNAME_KEEP_ALIVE = "keepAliveInterval"; //$NON-NLS-1$
	public static final String PARAMNAME_DATA_ATTRIBUTE_NAME = "dataAttributeName"; //$NON-NLS-1$

	static Logger TRACE = Logger.getLogger(AbstractMqttOperator.class);

	private static final Logger LOG = Logger.getLogger(LoggerNames.LOG_FACILITY
			+ "." + AbstractMqttOperator.class.getName()); //$NON-NLS-1$

	private String serverUri;
	private String connectionDocument;
	private String connection;
	private String trustStore;
	private String trustStorePassword;
	private String keyStore;
	private String keyStorePassword;
	
	private String clientID;
	private String userID;
	private String password;
	
	private String dataAttributeName;
	
	private long commandTimeout = IMqttConstants.UNINITIALIZED_COMMAND_TIMEOUT;
	private int keepAliveInterval = IMqttConstants.UNINITIALIZED_KEEP_ALIVE_INTERVAL;

	public AbstractMqttOperator() {
		super();
	}

	@ContextCheck(compile = true, runtime = false)
	public static void checkParams(OperatorContextChecker checker) {

		Set<String> parameterNames = checker.getOperatorContext()
				.getParameterNames();
		if (!parameterNames.contains(PARAMNAME_SERVER_URI)
				&& !parameterNames.contains(PARAMNAME_CONNECTION)) {
			checker.setInvalidContext(
					Messages.getString("Error_AbstractMqttOperator.7"), new Object[] {}); //$NON-NLS-1$
		}

		checker.checkExcludedParameters(PARAMNAME_SERVER_URI,
				PARAMNAME_CONNECTION, PARAMNAME_CONNDOC);
		checker.checkExcludedParameters(PARAMNAME_CONNECTION,
				PARAMNAME_SERVER_URI);
		checker.checkDependentParameters(PARAMNAME_CONNDOC,
				PARAMNAME_CONNECTION);
		
		checker.checkDependentParameters(PARAMNAME_USER_ID, PARAMNAME_PASSWORD);
		checker.checkDependentParameters(PARAMNAME_PASSWORD, PARAMNAME_USER_ID);

	}
	
	@ContextCheck(compile=false)
	public static void runtimeCheckParameterValue(OperatorContextChecker checker) {
		
		validateStringNotNullOrEmpty(checker, PARAMNAME_CLIENT_ID);
    	validateStringNotNullOrEmpty(checker, PARAMNAME_USER_ID);
    	validateStringNotNullOrEmpty(checker, PARAMNAME_PASSWORD);
    	
    	validateNumber(checker, PARAMNAME_COMMAND_TIMEOUT, 0, Long.MAX_VALUE);
    	validateNumber(checker, PARAMNAME_KEEP_ALIVE, 0, Integer.MAX_VALUE);
	}

	@Parameter(name = PARAMNAME_SERVER_URI, description = SPLDocConstants.MQTTSRC_PARAM_SERVERIURI_DESC, optional = true)
	public void setServerUri(String serverUri) {
		this.serverUri = serverUri;
	}

	public String getServerUri() {
		return serverUri;
	}

	public String getConnection() {
		return connection;
	}

	@Parameter(name = PARAMNAME_CONNECTION, description = SPLDocConstants.PARAM_CONNECTION_DESC, optional = true)
	public void setConnection(String connection) {
		this.connection = connection;
	}

	public String getConnectionDocument() {
		return connectionDocument;
	}

	@Parameter(name = PARAMNAME_CONNDOC, description = SPLDocConstants.PARAM_CONNDOC_DESC, optional = true)
	public void setConnectionDocument(String connectionDocument) {
		this.connectionDocument = connectionDocument;
	}

	protected void initFromConnectionDocument() throws Exception {

		// serverUri and connection - at least once must exist
		// only read from connection document if connection is specified
		// only initialize from connection document if parameter is not
		// already initialized

		// if serverUri is null, read connection document
		if (getConnection() != null) {
			ConnectionDocumentHelper helper = new ConnectionDocumentHelper();
			String connDoc = getConnectionDocument();

			// if connection document is not specified, default to
			// ../etc/connections.xml
			if (connDoc == null) {
				File appDir = getOperatorContext().getPE()
						.getApplicationDirectory();
				connDoc = appDir.getAbsolutePath()
						+ "/etc/connections.xml"; //$NON-NLS-1$
			}

			// convert from relative path to absolute path is necessary
			if (!connDoc.startsWith("/")) //$NON-NLS-1$
			{
				File appDir = getOperatorContext().getPE()
						.getApplicationDirectory();
				connDoc = appDir.getAbsolutePath() + "/" + connDoc; //$NON-NLS-1$
			}

			try {
				helper.parseAndValidateConnectionDocument(connDoc);
				ConnectionSpecification connectionSpecification = helper
						.getConnectionSpecification(getConnection());
				if (connectionSpecification != null) {
					setServerUri(connectionSpecification.getServerUri());

					String trustStore = connectionSpecification.getTrustStore();
					String trustStorePw = connectionSpecification
							.getTrustStorePassword();
					String keyStore = connectionSpecification.getKeyStore();
					String keyStorePw = connectionSpecification
							.getKeyStorePassword();
					String userID = connectionSpecification.getUserID();
					String password = connectionSpecification.getPassword();
					int keepAliveInterval = connectionSpecification.getKeepAliveInterval();
					long commandTimeout = connectionSpecification.getCommandTimeout();

					if (getTrustStore() == null)
						setTrustStore(trustStore);

					if (getKeyStore() == null)
						setKeyStore(keyStore);

					if (getKeyStorePassword() == null)
						setKeyStorePassword(keyStorePw);

					if (getTrustStorePassword() == null)
						setTrustStorePassword(trustStorePw);
					if (getUserID() == null)
						setUserID(userID);
					if (getPassword() == null) 
						setPassword(password);
					if(getKeepAliveInterval() == IMqttConstants.UNINITIALIZED_KEEP_ALIVE_INTERVAL && keepAliveInterval > IMqttConstants.UNINITIALIZED_KEEP_ALIVE_INTERVAL )
						setKeepAliveInterval(keepAliveInterval);
					if(this.getCommandTimeout() == IMqttConstants.UNINITIALIZED_COMMAND_TIMEOUT && commandTimeout > IMqttConstants.UNINITIALIZED_COMMAND_TIMEOUT)
						this.setCommandTimeout(commandTimeout);
				} else {
					TRACE.log(
							TraceLevel.ERROR,
							Messages.getString("Error_AbstractMqttOperator.3") + getConnection()); //$NON-NLS-1$
					LOG.log(LogLevel.ERROR,
							Messages.getString("Error_AbstractMqttOperator.3") + getConnection()); //$NON-NLS-1$
					throw new RuntimeException(
							Messages.getString("Error_AbstractMqttOperator.5")); //$NON-NLS-1$
				}
			} catch (SAXException e) {
				TRACE.log(LogLevel.ERROR,
						Messages.getString("Error_AbstractMqttOperator.6")); //$NON-NLS-1$
				throw e;
			} catch (IOException e) {
				TRACE.log(LogLevel.ERROR,
						Messages.getString("Error_AbstractMqttOperator.6")); //$NON-NLS-1$
				throw e;
			}
		}
	}

	protected void setupSslProperties(MqttClientWrapper client) {
		String trustStore = getTrustStore();
		String trustStorePw = getTrustStorePassword();
		String keyStore = getKeyStore();
		String keyStorePw = getKeyStorePassword();

		if (trustStore != null || keyStore != null) {
			Properties sslProperties = new Properties();

			if (trustStore != null) {
				sslProperties.setProperty(IMqttConstants.SSL_TRUST_STORE,
						trustStore);
			}

			if (keyStore != null) {
				sslProperties.setProperty(IMqttConstants.SSL_KEY_STORE,
						keyStore);
			}

			if (keyStorePw != null) {
				sslProperties.setProperty(
						IMqttConstants.SSL_KEY_STORE_PASSWORD, keyStorePw);
			}

			if (trustStorePw != null) {
				sslProperties.setProperty(
						IMqttConstants.SSK_TRUST_STORE_PASSWORD, trustStorePw);
			}
			client.setSslProperties(sslProperties);
		}
	}

	public String getTrustStore() {
		return toAbsolute(trustStore);
	}

	@Parameter(name = PARAMNAME_TRUST_STORE, optional = true, description = SPLDocConstants.PARAM_TRUSTORE_DESC)
	public void setTrustStore(String trustStore) {
		this.trustStore = trustStore;
	}

	public String getKeyStore() {
		return toAbsolute(keyStore);
	}

	@Parameter(name = PARAMNAME_KEY_STORE, optional = true, description = SPLDocConstants.PARAM_KEYSTORE_DESC)
	public void setKeyStore(String keyStore) {
		this.keyStore = keyStore;
	}

	public String getKeyStorePassword() {
		return keyStorePassword;
	}

	@Parameter(name = PARAMNAME_KEY_STORE_PASSWORD, optional = true, description = SPLDocConstants.PARAM_KEYSTORE_PW_DESC)
	public void setKeyStorePassword(String keyStorePassword) {
		this.keyStorePassword = keyStorePassword;
	}

	public String getTrustStorePassword() {
		return trustStorePassword;
	}

	@Parameter(name = PARAMNAME_TRUST_STORE_PASSWORD, optional = true, description = SPLDocConstants.PARAM_TRUSTORE_PW_DESC)
	public void setTrustStorePassword(String trustStorePassword) {
		this.trustStorePassword = trustStorePassword;
	}
	
    public String getClientID() {
		return clientID;
	}

    @Parameter(name=PARAMNAME_CLIENT_ID, description = SPLDocConstants.MQTT_PARAM_CLIENT_ID_DESC, optional=true)
	public void setClientID(String clientID) {
		this.clientID = clientID;
	}

    public String getUserID() {
		return userID;
	}

	@Parameter(name=PARAMNAME_USER_ID, description = SPLDocConstants.MQTT_PARAM_USER_ID_DESC, optional=true)
	public void setUserID(String userID) {
		this.userID = userID;
	}
	
	public String getPassword() {
		return password;
	}

	@Parameter(name=PARAMNAME_PASSWORD, description = SPLDocConstants.MQTT_PARAM_PASSWORD_DESC, optional=true)
	public void setPassword(String password) {
		this.password = password;
	}

	public long getCommandTimeout() {
		return commandTimeout;
	}

	@Parameter(name=PARAMNAME_COMMAND_TIMEOUT, description = SPLDocConstants.MQTT_PARAM_COMMAND_TIMEOUT_DESC, optional=true)
	public void setCommandTimeout(long commandTimeout) {
		this.commandTimeout = commandTimeout;
	}

	public int getKeepAliveInterval() {
		return keepAliveInterval;
	}

	@Parameter(name=PARAMNAME_KEEP_ALIVE, description = SPLDocConstants.MQTT_PARAM_KEEP_ALIVE_INTERVAL_DESC, optional=true)
	public void setKeepAliveInterval(int keepAliveInterval) {
		this.keepAliveInterval = keepAliveInterval;
	}

	public String getDataAttributeName() {
		return dataAttributeName;
	}
    
	@Parameter(name=PARAMNAME_DATA_ATTRIBUTE_NAME, description = SPLDocConstants.MQTT_PARAM_DATA_ATTRIBUTE_DESC, optional=true)
	public void setDataAttributeName(String dataAttributeName) {
		this.dataAttributeName = dataAttributeName;
	}

	protected static void validateStringNotNullOrEmpty(OperatorContextChecker checker, String parameterName) {
		if ((checker.getOperatorContext().getParameterNames().contains(parameterName))) {
			String value = checker.getOperatorContext().getParameterValues(parameterName).get(0);
			
			if(value == null || value.trim().length() == 0) {
				checker.setInvalidContext(Messages.getString("Error_AbstractMqttOperator.11"), new Object[] {parameterName});
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
			checker.setInvalidContext(
					Messages.getString("Error_AbstractMqttOperator.1"), //$NON-NLS-1$
					new Object[] { parameterName });
		}
	}

	protected static void validateSchemaForErrorOutputPort(
		OperatorContextChecker checker,
		StreamingOutput<OutputTuple> errorPort) {
	
		if (errorPort != null)
		{
			Set<String> parameterNames = checker.getOperatorContext()
					.getParameterNames();
	
			// if error port is present, check that it has the right schema
			// if there is not output parameter for error message, make sure
			// it's only
			// one attribute of ustring or rstring
			if (!parameterNames.contains(PARAMNAME_ERROR_OUT_ATTR_NAME)) {
				StreamSchema streamSchema = errorPort.getStreamSchema();
				int attrCount = streamSchema.getAttributeCount();
	
				if (attrCount > 1) {
					checker.setInvalidContext(
							Messages.getString("Error_MqttSourceOperator.6"), new Object[] {}); //$NON-NLS-1$
				}
	
				checker.checkAttributeType(streamSchema.getAttribute(0),
						MetaType.RSTRING, MetaType.USTRING);
			}
		}
	}

	protected String toAbsolute(String path) {
		if (path != null && !path.startsWith("/")) //$NON-NLS-1$
		{
			File appDir = getOperatorContext().getPE().getApplicationDirectory();
			return appDir.getAbsolutePath() + "/" + path; //$NON-NLS-1$
		}
		return path;
	}

	/**
	 * @return error output port if present, null if not specified
	 */
	abstract protected StreamingOutput<OutputTuple> getErrorOutputPort();
	
	protected void submitToErrorPort(String errorMsg, ConsistentRegionContext crContext) {
		StreamingOutput<OutputTuple> errorOutputPort = getErrorOutputPort();
		if (errorOutputPort != null) {
			OutputTuple errorTuple = errorOutputPort.newTuple();

			errorTuple.setString(0, errorMsg);

			try {
				if(crContext != null) {
					crContext.acquirePermit();
				}
				errorOutputPort.submit(errorTuple);
			} catch (Exception e) {
				TRACE.log(TraceLevel.ERROR,
						Messages.getString("Error_AbstractMqttOperator.10"), e); //$NON-NLS-1$
			} finally {
				if(crContext != null) {
					crContext.releasePermit();
				}
			}
		}
	}
}
