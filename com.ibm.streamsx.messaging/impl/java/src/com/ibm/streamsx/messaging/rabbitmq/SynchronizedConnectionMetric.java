package com.ibm.streamsx.messaging.rabbitmq;

import com.ibm.streams.operator.metrics.Metric;

public class SynchronizedConnectionMetric {
	Metric isConnected;
	Metric reconnectionAttempts;
	
	public void setReconnectionAttempts(Metric reconnectionAttempts) {
		this.reconnectionAttempts = reconnectionAttempts;
	}

	public SynchronizedConnectionMetric(Metric isConnected) {
		this.isConnected = isConnected;
	}

	public long getValue() {
		return isConnected.getValue();
	}

	public void setValue(long value) {
		synchronized(isConnected){
			isConnected.setValue(value);
			isConnected.notifyAll();
			
			// Increment our reconnection attempts every 
			// time we set isConnected to 0
			// Reset the reconnection attempts 
			// every time we successfully connect
			if (value == 0){
				reconnectionAttempts.increment();
			} else {
				reconnectionAttempts.setValue(0);
			}
		}
	}
	
	public void waitForMetricChange() throws InterruptedException{
		synchronized(isConnected){
			isConnected.wait();
		}
	}

}
