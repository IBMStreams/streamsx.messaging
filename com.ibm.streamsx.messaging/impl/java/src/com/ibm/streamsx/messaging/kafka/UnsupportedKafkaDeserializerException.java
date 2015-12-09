package com.ibm.streamsx.messaging.kafka;

public class UnsupportedKafkaDeserializerException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public UnsupportedKafkaDeserializerException(String message){
		super(message);
	}
}
