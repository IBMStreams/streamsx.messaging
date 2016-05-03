package com.ibm.streamsx.messaging.rabbitmq;

import java.util.logging.Logger;

import com.ibm.streams.operator.logging.TraceLevel;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.TopologyRecoveryException;

public class RabbitMQConnectionExceptionHandler implements ExceptionHandler {
	private final Logger trace = Logger.getLogger(RabbitMQSource.class
			.getCanonicalName());
	
	@Override
	public void handleBlockedListenerException(Connection arg0, Throwable arg1) {
		arg1.printStackTrace();
		trace.log(TraceLevel.ERROR, "See standard out for full stack trace. RabbitMQ Error message: " + arg1.getMessage());
	}

	@Override
	public void handleChannelRecoveryException(Channel arg0, Throwable arg1) {
		arg1.printStackTrace();
		trace.log(TraceLevel.ERROR, "See standard out for full stack trace. RabbitMQ Error message: " + arg1.getMessage());
	}

	@Override
	public void handleConfirmListenerException(Channel arg0, Throwable arg1) {
		arg1.printStackTrace();
		trace.log(TraceLevel.ERROR, "See standard out for full stack trace. RabbitMQ Error message: " + arg1.getMessage());
	}

	@Override
	public void handleConnectionRecoveryException(Connection arg0, Throwable arg1) {
		arg1.printStackTrace();
		trace.log(TraceLevel.ERROR, "See standard out for full stack trace. RabbitMQ Error message: " + arg1.getMessage());
	}

	@Override
	public void handleConsumerException(Channel arg0, Throwable arg1, Consumer arg2, String arg3, String arg4) {
		arg1.printStackTrace();
		trace.log(TraceLevel.ERROR, "See standard out for full stack trace. RabbitMQ Error message: " + arg1.getMessage());
	}

	@Override
	public void handleFlowListenerException(Channel arg0, Throwable arg1) {
		arg1.printStackTrace();
		trace.log(TraceLevel.ERROR, "See standard out for full stack trace. RabbitMQ Error message: " + arg1.getMessage());
	}

	@Override
	public void handleReturnListenerException(Channel arg0, Throwable arg1) {
		arg1.printStackTrace();
		trace.log(TraceLevel.ERROR, "See standard out for full stack trace. RabbitMQ Error message: " + arg1.getMessage());
	}

	@Override
	public void handleTopologyRecoveryException(Connection arg0, Channel arg1, TopologyRecoveryException arg2) {
		arg2.printStackTrace();
		trace.log(TraceLevel.ERROR, "See standard out for full stack trace. RabbitMQ Error message: " + arg2.getMessage());
	}

	@Override
	public void handleUnexpectedConnectionDriverException(Connection arg0, Throwable arg1) {
		arg1.printStackTrace();
		trace.log(TraceLevel.ERROR, "See standard out for full stack trace. RabbitMQ Error message: " + arg1.getMessage());
	}

}
