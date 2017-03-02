/*******************************************************************************
 * Copyright (C) 2015, MOHAMED-ALI SAID and International Business Machines
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.messaging.rabbitmq;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.logging.LogLevel;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.model.CustomMetric;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streamsx.messaging.common.PropertyProvider;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Recoverable;

@Libraries({ "opt/downloaded/*"/*, "@RABBITMQ_HOME@" */})
public class RabbitMQBaseOper extends AbstractOperator {

	protected Channel channel;
	protected Connection connection;
	protected String username = "", //$NON-NLS-1$
			password = "",  //$NON-NLS-1$
			exchangeName = "", exchangeType = "direct"; //$NON-NLS-1$ //$NON-NLS-2$
			
	protected List<String> hostAndPortList = new ArrayList<String>();
	protected Address[] addressArr; 
	private String vHost;
	private Boolean autoRecovery = true;
	private AtomicBoolean shuttingDown = new AtomicBoolean(false);
	protected boolean readyForShutdown = true;

	protected AttributeHelper messageHeaderAH = new AttributeHelper("message_header"), //$NON-NLS-1$
			routingKeyAH = new AttributeHelper("routing_key"), //$NON-NLS-1$
			messageAH = new AttributeHelper("message"); //$NON-NLS-1$

	private final static Logger trace = Logger.getLogger(RabbitMQBaseOper.class
			.getCanonicalName());
	protected Boolean usingDefaultExchange = false;
	private String URI = ""; //$NON-NLS-1$
	private long networkRecoveryInterval = 5000;
	
	protected SynchronizedConnectionMetric isConnected;
	private Metric reconnectionAttempts;
	private String appConfigName = ""; //$NON-NLS-1$
	private String userPropName;
	private String passwordPropName;
	
	
	/*
	 * The method checkParametersRuntime validates that the reconnection policy
	 * parameters are appropriate
	 */
	@ContextCheck(compile = false)
	public static void checkParametersRuntime(OperatorContextChecker checker) {		
		if((checker.getOperatorContext().getParameterNames().contains("appConfigName"))) { //$NON-NLS-1$
        	String appConfigName = checker.getOperatorContext().getParameterValues("appConfigName").get(0); //$NON-NLS-1$
			String userPropName = checker.getOperatorContext().getParameterValues("userPropName").get(0); //$NON-NLS-1$
			String passwordPropName = checker.getOperatorContext().getParameterValues("passwordPropName").get(0); //$NON-NLS-1$
			
			
			PropertyProvider provider = new PropertyProvider(checker.getOperatorContext().getPE(), appConfigName);
			
			String userName = provider.getProperty(userPropName);
			String password = provider.getProperty(passwordPropName);
			
			if(userName == null || userName.trim().length() == 0) {
				trace.log(LogLevel.ERROR, Messages.getString("PROPERTY_NOT_FOUND_IN_APP_CONFIG", userPropName, appConfigName)); //$NON-NLS-1$
				checker.setInvalidContext(
						Messages.getString("PROPERTY_NOT_FOUND_IN_APP_CONFIG"), //$NON-NLS-1$
						new Object[] {userPropName, appConfigName});
				
			}
			
			if(password == null || password.trim().length() == 0) {
				trace.log(LogLevel.ERROR, Messages.getString("PROPERTY_NOT_FOUND_IN_APP_CONFIG", passwordPropName, appConfigName)); //$NON-NLS-1$
				checker.setInvalidContext(
						Messages.getString("PROPERTY_NOT_FOUND_IN_APP_CONFIG"), //$NON-NLS-1$
						new Object[] {passwordPropName, appConfigName});
			
			}
        }
	}
	
	// add check for appConfig userPropName and passwordPropName
	@ContextCheck(compile = true)
	public static void checkParameters(OperatorContextChecker checker) {	
		// Make sure if appConfigName is specified then both userPropName and passwordPropName are needed
		checker.checkDependentParameters("appConfigName", "userPropName", "passwordPropName"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		checker.checkDependentParameters("userPropName", "appConfigName", "passwordPropName"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		checker.checkDependentParameters("passwordPropName", "appConfigName", "userPropName"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	public synchronized void initialize(OperatorContext context)
			throws Exception {
		isConnected.setReconnectionAttempts(reconnectionAttempts);
		// Must call super.initialize(context) to correctly setup an operator.
		super.initialize(context);
	}
	
	protected boolean newCredentialsExist() {
		PropertyProvider propertyProvider = null;
		boolean newProperties = false;
		
		if (!getAppConfigName().isEmpty()) {
			OperatorContext context = getOperatorContext();
			propertyProvider = new PropertyProvider(context.getPE(), getAppConfigName());
			if (propertyProvider.contains(userPropName)
					&& !username.equals(propertyProvider.getProperty(userPropName))) {
				newProperties = true;
			}
			if (propertyProvider.contains(passwordPropName)
					&& !password.equals(propertyProvider.getProperty(passwordPropName))) {
				newProperties = true;
			}
		}
		
		trace.log(TraceLevel.INFO,
				"newPropertiesExist() is returning a value of: " + newProperties); //$NON-NLS-1$
		
		
		return newProperties;
	}
	
	public void resetRabbitClient() throws KeyManagementException, MalformedURLException, NoSuchAlgorithmException, URISyntaxException, IOException, TimeoutException, InterruptedException, Exception {
		if (autoRecovery){
			trace.log(TraceLevel.WARN, "Resetting Rabbit Client."); //$NON-NLS-1$
			closeRabbitConnections();
			initializeRabbitChannelAndConnection();
			
		} else {
			trace.log(TraceLevel.INFO, "AutoRecovery was not enabled, so we are not resetting client."); //$NON-NLS-1$
		}
	}
	

	/*
	 * Setup connection and channel. If automatic recovery is enabled, we will reattempt 
	 * to connect every networkRecoveryInterval
	 */
	public void initializeRabbitChannelAndConnection() throws MalformedURLException, URISyntaxException, NoSuchAlgorithmException,
			KeyManagementException, IOException, TimeoutException, InterruptedException, OperatorShutdownException, FailedToConnectToRabbitMQException {
		do {
			try {
				ConnectionFactory connectionFactory = setupConnectionFactory();
				
				// If we return from this without throwing an exception, then we 
				// have successfully connected
				connection = setupNewConnection(connectionFactory, URI, addressArr, isConnected);
				
				
				channel = initializeExchange();
				
				isConnected.setValue(1);
				
				trace.log(TraceLevel.INFO,
						"Initializing channel connection to exchange: " + exchangeName //$NON-NLS-1$
								+ " of type: " + exchangeType + " as user: " + connectionFactory.getUsername()); //$NON-NLS-1$ //$NON-NLS-2$
				trace.log(TraceLevel.INFO,
						"Connection to host: " + connection.getAddress()); //$NON-NLS-1$
			} catch (IOException | TimeoutException e) {
				e.printStackTrace();
				trace.log(LogLevel.ERROR, Messages.getString("FAILED_TO_SETUP_CONNECTION", e.getMessage())); //$NON-NLS-1$
				if (autoRecovery == true){
					Thread.sleep(networkRecoveryInterval);
				}
			}
		} while (autoRecovery == true 
				&& (connection == null || channel == null)
				&& !shuttingDown.get());
		
		if (connection == null || channel == null){
			throw new FailedToConnectToRabbitMQException(Messages.getString("FAILED_TO_INIT_CONNECTION_OR_CHANNEL_TO_SERVER")); //$NON-NLS-1$
		}
	}

	private ConnectionFactory setupConnectionFactory()
			throws MalformedURLException, URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
		ConnectionFactory connectionFactory = new ConnectionFactory();
		connectionFactory.setExceptionHandler(new RabbitMQConnectionExceptionHandler(isConnected));
		connectionFactory.setAutomaticRecoveryEnabled(autoRecovery);
		if (autoRecovery){
			connectionFactory.setNetworkRecoveryInterval(networkRecoveryInterval);
		}
		
		if (URI.isEmpty()){
			configureUsernameAndPassword(connectionFactory);
			if (vHost != null)
				connectionFactory.setVirtualHost(vHost);
			
			addressArr = buildAddressArray(hostAndPortList);
			
		} else{
			//use specified URI rather than username, password, vHost, hostname, etc
			if (!username.isEmpty() | !password.isEmpty() | vHost != null | !hostAndPortList.isEmpty()){
				trace.log(TraceLevel.WARNING, "You specified a URI, therefore username, password" //$NON-NLS-1$
						+ ", vHost, and hostname parameters will be ignored."); //$NON-NLS-1$
			}
			connectionFactory.setUri(URI);
		}
		return connectionFactory;
	}

	/*
	 * Attempts to make a connection and throws an exception if it fails 
	 */
	private Connection setupNewConnection(ConnectionFactory connectionFactory, String URI, Address[] addressArr, SynchronizedConnectionMetric isConnected2) throws IOException, TimeoutException, InterruptedException, OperatorShutdownException {
		Connection connection = null;
		connection = getConnection(connectionFactory, URI, addressArr);
		if (connectionFactory.isAutomaticRecoveryEnabled()) {
			((Recoverable) connection).addRecoveryListener(new AutoRecoveryListener(isConnected2));
		}

		return connection;
	}

	private Connection getConnection(ConnectionFactory connectionFactory, String URI, Address[] addressArr)
			throws IOException, TimeoutException {
		Connection connection;
		if (URI.isEmpty()){
			connection = connectionFactory.newConnection(addressArr);
			trace.log(TraceLevel.INFO, "Creating a new connection based on an address list."); //$NON-NLS-1$
		} else {
			connection = connectionFactory.newConnection();
			trace.log(TraceLevel.INFO, "Creating a new connection based on a provided URI."); //$NON-NLS-1$
		}
		return connection;
	}

	/*
	 * Set the username and password either from the parameters provided or from
	 * the appConfig. appConfig credentials have higher precedence than parameter credentials. 
	 */
	private void configureUsernameAndPassword(ConnectionFactory connectionFactory) {
		// Lowest priority parameters first
		// We overwrite those values if we find them in the appConfig
		PropertyProvider propertyProvider = null;
		
		// Overwrite with appConfig values if present. 
		if (!getAppConfigName().isEmpty()) {
			OperatorContext context = getOperatorContext();
			propertyProvider = new PropertyProvider(context.getPE(), getAppConfigName());
			if (propertyProvider.contains(userPropName)) {
				username = propertyProvider.getProperty(userPropName);
			}
			if (propertyProvider.contains(passwordPropName)) {
				password = propertyProvider.getProperty(passwordPropName);
			}
		}

		if (!username.isEmpty()) {
			connectionFactory.setUsername(username);
			trace.log(TraceLevel.INFO, "Set username."); //$NON-NLS-1$
		} else {
			trace.log(TraceLevel.INFO,
					"Default username: " + connectionFactory.getUsername() ); //$NON-NLS-1$
		}
		
		if (!password.isEmpty()) {
			connectionFactory.setPassword(password);
			trace.log(TraceLevel.INFO, "Set password."); //$NON-NLS-1$
		} else {
			trace.log(TraceLevel.INFO,
					"Default password: " + connectionFactory.getPassword()); //$NON-NLS-1$
		}
	}

	private Channel initializeExchange() throws IOException {
		Channel channel = connection.createChannel();
		try{
			//check to see if the exchange exists if not then it is the default exchange
			if ( !exchangeName.isEmpty()){
				channel.exchangeDeclarePassive(exchangeName);
				trace.log(TraceLevel.INFO, "Exchange was found, therefore no exchange will be declared."); //$NON-NLS-1$
			} else {
				usingDefaultExchange = true;
				trace.log(TraceLevel.INFO, "Using the default exchange. Name \"\""); //$NON-NLS-1$
			}
		} catch (IOException e){
			// if exchange doesn't exist, we will create it
			// we must also create a new channel since last one erred
			channel = connection.createChannel();
			// declare non-durable, auto-delete exchange
			channel.exchangeDeclare(exchangeName, exchangeType, false, true, null);
			trace.log(TraceLevel.INFO, "Exchange was not found, therefore non-durable exchange will be declared."); //$NON-NLS-1$
		}
		return channel;
	}

	private Address[] buildAddressArray(List<String> hostsAndPorts) throws MalformedURLException {
		Address[] addrArr = new Address[hostsAndPorts.size()];
		int i = 0;
		for (String hostAndPort : hostsAndPorts){
			URL tmpURL = new URL("http://" + hostAndPort); //$NON-NLS-1$
			addrArr[i++] = new Address(tmpURL.getHost(), tmpURL.getPort());
			trace.log(TraceLevel.INFO, "Adding: " + tmpURL.getHost() + ":"+ tmpURL.getPort()); //$NON-NLS-1$ //$NON-NLS-2$
		}
		trace.log(TraceLevel.INFO, "Built address array: \n" + addrArr.toString()); //$NON-NLS-1$
		
		return addrArr;
	}

	public void shutdown() throws Exception {
		shuttingDown.set(true);
		closeRabbitConnections();
		// Need this to make sure that we return from the process method
		// before exiting shutdown
		while(!readyForShutdown){
			Thread.sleep(100);
		}
		super.shutdown();
	}


	private void closeRabbitConnections() {
		if (channel != null){
			try {
				channel.close();
			} catch (Exception e){
				e.printStackTrace();
				trace.log(LogLevel.ALL, Messages.getString("EXCEPTION_AT_CHANNEL_CLOSE", e.toString())); //$NON-NLS-1$
			} finally {
				channel = null;
			}
		}
				
		if (connection != null){
			try {
				connection.close();
			} catch (Exception e) {
				e.printStackTrace();
				trace.log(LogLevel.ALL, Messages.getString("EXCEPTION_AT_CONNECTION_CLOSE", e.toString())); //$NON-NLS-1$
			} finally {
				connection = null;
			}
		}
		
		isConnected.setValue(0);
	}

	public void initSchema(StreamSchema ss) throws Exception {
		Set<MetaType> supportedTypes = new HashSet<MetaType>();
		supportedTypes.add(MetaType.MAP);
		messageHeaderAH.initialize(ss, false, supportedTypes);
		supportedTypes.remove(MetaType.MAP);
		
		supportedTypes.add(MetaType.RSTRING);
		supportedTypes.add(MetaType.USTRING);
		
		routingKeyAH.initialize(ss, false, supportedTypes);
		
		supportedTypes.add(MetaType.BLOB);
		
		messageAH.initialize(ss, true, supportedTypes);

	}
	


	@Parameter(optional = true, description = "List of host and port in form: \\\"myhost1:3456\\\",\\\"myhost2:3456\\\".")
	public void setHostAndPort(List<String> value) {
		hostAndPortList.addAll(value);
	}

	@Parameter(optional = true, description = "Username for RabbitMQ authentication.")
	public void setUsername(String value) {
		username = value;
	}

	@Parameter(optional = true, description = "Password for RabbitMQ authentication.")
	public void setPassword(String value) {
		password = value;
	}
	
	@Parameter(optional = true, description = "This parameter specifies the name of application configuration that stores client credentials, "
			+ "the property specified via application configuration is overridden by the application parameters. "
			+ "The hierarchy of credentials goes: credentials from the appConfig beat out parameters (username and password). "
			+ "The valid key-value pairs in the appConfig are <userPropName>=<username> and <passwordPropName>=<password>, where "
			+ "<userPropName> and <passwordPropName> are specified by the corresponding parameters. "
			+ "If the operator loses connection to the RabbitMQ server, or it fails authentication, it will "
			+ "check for new credentials in the appConfig and attempt to reconnect if they exist. "
			+ "The attempted reconnection will only take place if automaticRecovery is set to true (which it is by default).")
	public void setAppConfigName(String appConfigName) {
		this.appConfigName  = appConfigName;
	}
	
	public String getAppConfigName() {
		return appConfigName;
	}
	
	@Parameter(optional = true, description = "This parameter specifies the property name of user name in the application configuration. If the appConfigName parameter is specified and the userPropName parameter is not set, a compile time error occurs.")
	public void setUserPropName(String userPropName) {
		this.userPropName = userPropName;
	}
    
	@Parameter(optional = true, description = "This parameter specifies the property name of password in the application configuration. If the appConfigName parameter is specified and the passwordPropName parameter is not set, a compile time error occurs.")
	public void setPasswordPropName(String passwordPropName) {
		this.passwordPropName = passwordPropName;
	}
	
	@Parameter(optional = true, description = "Optional attribute. Name of the RabbitMQ exchange type. Default direct.")
	public void setExchangeType(String value) {
		exchangeType = value;
	}
	
	@Parameter(optional = true, description = "Convenience URI of form: amqp://userName:password@hostName:portNumber/virtualHost. If URI is specified, you cannot specify username, password, and host.")
	public void setURI(String value) {
		URI  = value;
	}

	@Parameter(optional = true, description = "Name of the attribute for the message. Default is \\\"message\\\".")
	public void setMessageAttribute(String value) {
		messageAH.setName(value);
	}

	@Parameter(optional = true, description = "Name of the attribute for the routing_key. Default is \\\"routing_key\\\".")
	public void setRoutingKeyAttribute(String value) {
		routingKeyAH.setName(value);
	}

	@Parameter(optional = true, description = "Name of the attribute for the message_header. Schema of type must be Map<ustring,ustring>. Default is \\\"message_header\\\".")
	public void setMsgHeaderAttribute(String value) {
		messageHeaderAH.setName(value);
	}
	
	@Parameter(optional = true, description = "Set Virtual Host. Default is null.")
	public void setVirtualHost(String value) {
		vHost = value; 
	}
	
	@Parameter(optional = true, description = "Have connections to RabbitMQ automatically recovered. Default is true.")
	public void setAutomaticRecovery(Boolean value) {
		autoRecovery = value; 
	}
	
	public Boolean getAutoRecovery() {
		return autoRecovery;
	}

	@Parameter(optional = true, description = "If automaticRecovery is set to true, this is the interval (in ms) that will be used between reconnection attempts. The default is 5000 ms.")
	public void setSetNetworkRecoveryInterval(long value) {
		networkRecoveryInterval  = value; 
	}
	
	@CustomMetric(name = "isConnected", kind = Metric.Kind.GAUGE,
		    description = "Describes whether we are currently connected to the RabbitMQ server.")
	public void setIsConnectedMetric(Metric isConnected) {
		this.isConnected = new SynchronizedConnectionMetric(isConnected);
	}
	
	@CustomMetric(name = "reconnectionAttempts", kind = Metric.Kind.COUNTER,
		    description = "The number of times we have attempted to reconnect since the last successful connection.")
	public void setReconnectionAttempts(Metric reconnectionAttempts) {
		this.reconnectionAttempts = reconnectionAttempts;
	}
	
	
	public static final String BASE_DESC = 
 "\\n\\n**AppConfig**: " //$NON-NLS-1$
			+ "The hierarchy of credentials goes: credentials from the appConfig beat out parameters (username and password). " //$NON-NLS-1$
			+ "The valid key-value pairs in the appConfig are <userPropName>=<username> and <passwordPropName>=<password>, where " //$NON-NLS-1$
			+ "<userPropName> and <passwordPropName> are specified by the corresponding parameters. " //$NON-NLS-1$
			+ "This operator will only automatically recover with new credentials from the appConfig if automaticRecovery " //$NON-NLS-1$
			+ "is set to true. "; //$NON-NLS-1$


}
