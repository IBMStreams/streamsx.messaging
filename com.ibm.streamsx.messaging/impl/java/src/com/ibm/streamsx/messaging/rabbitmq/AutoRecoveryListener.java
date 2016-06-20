package com.ibm.streamsx.messaging.rabbitmq;

import java.util.logging.Logger;

import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.metrics.Metric;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;

public class AutoRecoveryListener implements RecoveryListener {
	private final Logger trace = Logger.getLogger(this.getClass()
			.getCanonicalName());
	private Metric isConnected;
	
	public AutoRecoveryListener(Metric isConnected) {
		this.isConnected = isConnected;
	}
	
	@Override
	public void handleRecovery(Recoverable arg0) {
		trace.log(TraceLevel.INFO, "Recovered RabbitMQ connection.");
		isConnected.setValue(1);
	}

}
