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

	private static final String APP_CONFIG_NAME = "appConfigName";
	private static final String PASSWORD = "password";
	private static final String USERNAME = "username";
	protected Channel channel;
	protected Connection connection;
	protected String usernameParameter = "",
			passwordParameter = "", 
			usernamePP = "", // username from propertyProvider
			passwordPP = "", // password from propertyProvider
			exchangeName = "", exchangeType = "direct";
			
	protected List<String> hostAndPortList = new ArrayList<String>();
	protected Address[] addressArr; 
	private String vHost;
	private Boolean autoRecovery = true;
	private AtomicBoolean shuttingDown = new AtomicBoolean(false);
	protected boolean readyForShutdown = true;

	protected AttributeHelper messageHeaderAH = new AttributeHelper("message_header"),
			routingKeyAH = new AttributeHelper("routing_key"),
			messageAH = new AttributeHelper("message");

	private final Logger trace = Logger.getLogger(this.getClass()
			.getCanonicalName());
	protected Boolean usingDefaultExchange = false;
	private String URI = "";
	private long networkRecoveryInterval = 5000;
	
	protected SynchronizedConnectionMetric isConnected;
	private Metric reconnectionAttempts;
	private String appConfigName = "";
	
	
	/*
	 * The method checkParametersRuntime validates that the reconnection policy
	 * parameters are appropriate
	 */
	@ContextCheck(compile = false)
	public static void checkParametersRuntime(OperatorContextChecker checker) {
		if((checker.getOperatorContext().getParameterNames().contains(APP_CONFIG_NAME))) {
        	String appConfigName = checker.getOperatorContext().getParameterValues(APP_CONFIG_NAME).get(0);
			
			@SuppressWarnings("unused") // If it is empty, we will throw an exception
			PropertyProvider provider = new PropertyProvider(checker.getOperatorContext().getPE(), appConfigName);
        }
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
			if (propertyProvider.contains(USERNAME)
					&& !usernamePP.equals(propertyProvider.getProperty(USERNAME))) {
				newProperties = true;
			}
			if (propertyProvider.contains(PASSWORD)
					&& !passwordPP.equals(propertyProvider.getProperty(PASSWORD))) {
				newProperties = true;
			}
		}
		
		trace.log(TraceLevel.INFO,
				"newPropertiesExist() is returning a value of: " + newProperties);
		
		
		return newProperties;
	}
	
	public void resetRabbitClient() throws KeyManagementException, MalformedURLException, NoSuchAlgorithmException, URISyntaxException, IOException, TimeoutException, InterruptedException, Exception {
		if (autoRecovery){
			trace.log(TraceLevel.WARN, "Resetting Rabbit Client.");
			closeRabbitConnections();
			initializeRabbitChannelAndConnection();
			
		} else {
			trace.log(TraceLevel.INFO, "AutoRecovery was not enabled, so we are not resetting client.");
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
						"Initializing channel connection to exchange: " + exchangeName
								+ " of type: " + exchangeType + " as user: " + connectionFactory.getUsername());
				trace.log(TraceLevel.INFO,
						"Connection to host: " + connection.getAddress());
			} catch (IOException | TimeoutException e) {
				e.printStackTrace();
				trace.log(TraceLevel.ERROR, "Failed to setup connection: " + e.getMessage());
				if (autoRecovery == true){
					Thread.sleep(networkRecoveryInterval);
				}
			}
		} while (autoRecovery == true 
				&& (connection == null || channel == null)
				&& !shuttingDown.get());
		
		if (connection == null || channel == null){
			throw new FailedToConnectToRabbitMQException("Failed to initialize connection or channel to RabbitMQ Server.");
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
			if (!usernameParameter.isEmpty() | !passwordParameter.isEmpty() | vHost != null | !hostAndPortList.isEmpty()){
				trace.log(TraceLevel.WARNING, "You specified a URI, therefore username, password"
						+ ", vHost, and hostname parameters will be ignored.");
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
			trace.log(TraceLevel.INFO, "Creating a new connection based on an address list.");
		} else {
			connection = connectionFactory.newConnection();
			trace.log(TraceLevel.INFO, "Creating a new connection based on a provided URI.");
		}
		return connection;
	}

	/*
	 * Set the username and password either from the parameters provided or from
	 * the appConfig. Parameters beat appConfig.
	 */
	private void configureUsernameAndPassword(ConnectionFactory connectionFactory) {
		// Lowest priority PropertyProvider first
		// We only write to username and appConfig from appConfig if they're already set
		PropertyProvider propertyProvider = null;
		String configuredUsername = usernameParameter;
		String configuredPassword = passwordParameter;
		
		if (!getAppConfigName().isEmpty()) {
			OperatorContext context = getOperatorContext();
			propertyProvider = new PropertyProvider(context.getPE(), getAppConfigName());
			if (propertyProvider.contains(USERNAME)) {
				usernamePP = propertyProvider.getProperty(USERNAME);
				if (configuredUsername.isEmpty()){
					configuredUsername = usernamePP;
				}
			}
			if (propertyProvider.contains(PASSWORD)) {
				passwordPP = propertyProvider.getProperty(PASSWORD);
				if (configuredPassword.isEmpty()){
					configuredPassword = passwordPP;
				}
			}
		}

		if (!configuredUsername.isEmpty()) {
			connectionFactory.setUsername(configuredUsername);
			trace.log(TraceLevel.INFO, "Set username.");
		} else {
			trace.log(TraceLevel.INFO,
					"Default username: " + connectionFactory.getUsername() );
		}
		
		if (!configuredPassword.isEmpty()) {
			connectionFactory.setPassword(configuredPassword);
			trace.log(TraceLevel.INFO, "Set password.");
		} else {
			trace.log(TraceLevel.INFO,
					"Default password: " + connectionFactory.getPassword());
		}
	}

	private Channel initializeExchange() throws IOException {
		Channel channel = connection.createChannel();
		try{
			//check to see if the exchange exists if not then it is the default exchange
			if ( !exchangeName.isEmpty()){
				channel.exchangeDeclarePassive(exchangeName);
				trace.log(TraceLevel.INFO, "Exchange was found, therefore no exchange will be declared.");
			} else {
				usingDefaultExchange = true;
				trace.log(TraceLevel.INFO, "Using the default exchange. Name \"\"");
			}
		} catch (IOException e){
			// if exchange doesn't exist, we will create it
			// we must also create a new channel since last one erred
			channel = connection.createChannel();
			// declare non-durable, auto-delete exchange
			channel.exchangeDeclare(exchangeName, exchangeType, false, true, null);
			trace.log(TraceLevel.INFO, "Exchange was not found, therefore non-durable exchange will be declared.");
		}
		return channel;
	}

	private Address[] buildAddressArray(List<String> hostsAndPorts) throws MalformedURLException {
		Address[] addrArr = new Address[hostsAndPorts.size()];
		int i = 0;
		for (String hostAndPort : hostsAndPorts){
			URL tmpURL = new URL("http://" + hostAndPort);
			addrArr[i++] = new Address(tmpURL.getHost(), tmpURL.getPort());
			trace.log(TraceLevel.INFO, "Adding: " + tmpURL.getHost() + ":"+ tmpURL.getPort());
		}
		trace.log(TraceLevel.INFO, "Built address array: \n" + addrArr.toString());
		
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
				trace.log(TraceLevel.ALL, "Exception at channel close: " + e.toString());
			} finally {
				channel = null;
			}
		}
				
		if (connection != null){
			try {
				connection.close();
			} catch (Exception e) {
				e.printStackTrace();
				trace.log(TraceLevel.ALL, "Exception at connection close: " + e.toString());
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
		usernameParameter = value;
	}

	@Parameter(optional = true, description = "Password for RabbitMQ authentication.")
	public void setPassword(String value) {
		passwordParameter = value;
	}
	
	@Parameter(optional = true, description = "This parameter specifies the name of application configuration that stores client credentials, "
			+ "the property specified via application configuration is overridden by the application parameters. "
			+ "The hierarchy of properties goes: parameter (username and password) beats out appConfigName. "
			+ "The valid key-value pairs in the appConfig are username=<username> and password=<password>. "
			+ "If the operator loses connection to the RabbitMQ server, or it fails authentication, it will "
			+ "check for new credentials in the appConfig and attempt to reconnect if they exist. "
			+ "The attempted reconnection will only take place if automaticRecovery is set to true (which it is by default).")
	public void setAppConfigName(String appConfigName) {
		this.appConfigName  = appConfigName;
	}
	
	public String getAppConfigName() {
		return appConfigName;
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
			"\\n**AppConfig**: The valid key-value pairs in the appConfig are username=<username> and password=<password>. ";


}
