/*******************************************************************************
 * Copyright (C) 2015, MOHAMED-ALI SAID and International Business Machines
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.messaging.rabbitmq;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.Parameter;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

@Libraries({ "opt/downloaded/*" })
public class RabbitBaseOper extends AbstractOperator {

	protected Channel channel;
	protected Connection connection;
	protected String hostName = "localhost", username = "guest",
			password = "guest", exchangeName = "logs", exchangeType = "direct",
			queueName = "";
	protected int portId = 5672;

	protected AttributeHelper topicAH = new AttributeHelper("topic"),
			routingKeyAH = new AttributeHelper("routing_key"),
			messageAH = new AttributeHelper("message");

	protected final Logger trace = Logger.getLogger(RabbitBaseOper.class
			.getCanonicalName());

	public synchronized void initialize(OperatorContext context)
			throws Exception {
		// Must call super.initialize(context) to correctly setup an operator.
		super.initialize(context);
		ConnectionFactory connectionFactory = new ConnectionFactory();
		connectionFactory.setUsername(username);
		connectionFactory.setPassword(password);
		connectionFactory.setHost(hostName);
		connectionFactory.setPort(portId);
		connection = connectionFactory.newConnection();
		channel = connection.createChannel();
		channel.exchangeDeclare(exchangeName, exchangeType);
		trace.log(TraceLevel.INFO,
				"Initializing channel connection to exchange: " + exchangeName
						+ " of type: " + exchangeType + " as user: " + username);
	}

	public void shutdown() throws IOException, TimeoutException {
		channel.close();
		connection.close();
	}

	public void initSchema(StreamSchema ss) throws Exception {
		Set<MetaType> supportedTypes = new HashSet<MetaType>();
		supportedTypes.add(MetaType.RSTRING);
		supportedTypes.add(MetaType.USTRING);
		supportedTypes.add(MetaType.BLOB);

		routingKeyAH.initialize(ss, true, supportedTypes);
		messageAH.initialize(ss, true, supportedTypes);

	}

	@Parameter(optional = false, description = "Name of the attribute for the message. This attribute is required. Default is \\\"message\\\".")
	public void setHostname(String value) {
		hostName = value;
	}

	@Parameter(optional = false, description = "Name of the attribute for the key. Default is \\\"key\\\".")
	public void setUsername(String value) {
		username = value;
	}

	@Parameter(optional = false, description = "Name of the attribute for the key. Default is \\\"key\\\".")
	public void setPassword(String value) {
		password = value;
	}

	@Parameter(optional = true, description = "Exchange Name.")
	public void setExchangeName(String value) {
		exchangeName = value;
	}

	@Parameter(optional = true, description = "Name of the attribute for the key. Default is \\\"key\\\".")
	public void setQueueName(String value) {
		queueName = value;
	}

	@Parameter(optional = true, description = "Name of the attribute for the message. This attribute is required. Default is \\\"message\\\".")
	public void setMessageAttribute(String value) {
		messageAH.setName(value);
	}

	@Parameter(optional = true, description = "Port id. Default 5672.")
	public void setPortId(int value) {
		portId = value;
	}

	@Parameter(optional = true, description = "Exchange Name.")
	public void setRoutingKeyAttribute(String value) {
		routingKeyAH.setName(value);
	}

}
