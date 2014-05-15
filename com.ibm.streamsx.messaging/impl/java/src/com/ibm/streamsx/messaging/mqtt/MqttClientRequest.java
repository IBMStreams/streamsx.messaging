package com.ibm.streamsx.messaging.mqtt;



/**
 * Represent a client request
 *
 */
public class MqttClientRequest {
	
	public enum MqttClientRequestType {CONNECT, ADD_TOPICS, REMOVE_TOPICS, UPDATE_TOPICS, REPLACE_TOPICS};
	
	private MqttClientRequestType reqType;
	private String serverUri;
	private String[] topics;
	private int qos;
	
	
	public MqttClientRequestType getReqType() {
		return reqType;
	}
	public MqttClientRequest setReqType(MqttClientRequestType reqType) {
		this.reqType = reqType;
		return this;
	}
	public String getServerUri() {
		return serverUri;
	}
	public MqttClientRequest setServerUri(String serverUri) {
		this.serverUri = serverUri;
		return this;
	}
	public String[] getTopics() {
		return topics;
	}
	public MqttClientRequest setTopics(String[] topics) {
		this.topics = topics;
		return this;
	}
	public int getQos() {
		return qos;
	}
	public MqttClientRequest setQos(int qos) {
		this.qos = qos;
		return this;
	}

	
	
}
