/*******************************************************************************
 * Copyright (C) 2015, MOHAMED-ALI SAID
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.rabbitmq;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class RabbitMQWrapper {

	private static final org.slf4j.Logger log = LoggerFactory
			.getLogger(RabbitMQWrapper.class);
	private final String ExchangeName;
	private final String RoutingKey;
	private final String exchangeType;

	private UpdateEvent updateEvent;
	private ConnectionFactory connectionFactory;
	private com.rabbitmq.client.Connection connection;
	private Channel channel;

	public void login(String userName, String password, String hostName,
			int port) throws IOException, TimeoutException {
		connectionFactory = new ConnectionFactory();
		connectionFactory.setUsername(userName);
		connectionFactory.setPassword(password);
		connectionFactory.setHost(hostName);
		connectionFactory.setPort(port);
		connection = connectionFactory.newConnection();
		channel = connection.createChannel();
		channel.exchangeDeclare(ExchangeName, exchangeType);
	}

	public RabbitMQWrapper(final String exchangeName, final String exchangeT) {
		ExchangeName = exchangeName;
		RoutingKey = "";
		exchangeType = exchangeT;
	}
	
	

	public RabbitMQWrapper(UpdateEvent event, final String exchangeName,
			final String routingKey) {
		updateEvent = event;
		ExchangeName = exchangeName;
		RoutingKey = routingKey;
		exchangeType = "direct";
	}

	public void logout() {
		try {
			connection.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void publish(String routingKey, String message) {

		try {
			Channel channel = connection.createChannel();

			String queueName = channel.queueDeclare().getQueue();
			channel.queueBind(queueName, ExchangeName, routingKey);
			log.trace("Producing message: " + message + " in thread: "
					+ Thread.currentThread().getName());
			
			channel.basicPublish(ExchangeName, routingKey, null,
					message.getBytes());

			channel.close();

		} catch (IOException e) {
			log.trace("Exception message:" + e.getMessage() + "\r\n");
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void Consume() {

		try {
			boolean NO_ACK = false;
			String queueName = channel.queueDeclare().getQueue();
			channel.queueBind(queueName, ExchangeName, RoutingKey);
			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(queueName, NO_ACK, consumer);
			while (true) { // you might want to implement some loop-finishing
				// logic here ;)
				QueueingConsumer.Delivery delivery;
				try {
					delivery = consumer.nextDelivery();
					String Message = new String(delivery.getBody());
					log.trace("received message: " + Message + " in thread: "
							+ Thread.currentThread().getName());
					channel.basicAck(delivery.getEnvelope().getDeliveryTag(),
							false);
					updateEvent.NotifyUpdateEvent(RoutingKey, Message);
				} catch (InterruptedException ie) {
					continue;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
