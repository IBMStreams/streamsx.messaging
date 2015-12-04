package com.ibm.streamsx.messaging.kafka;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.logging.TraceLevel;

public abstract class KafkaProducerClient extends KafkaBaseClient {
	
	public KafkaProducerClient(AttributeHelper topicAH, AttributeHelper keyAH,
			AttributeHelper messageAH, Properties props){
		super(topicAH, keyAH, messageAH, props);
		setDefaultSerializers();
		
	}

	abstract void send(Tuple tuple, List<String> topics) throws Exception;

	abstract void send(Tuple tuple) throws Exception;	
	
	private void setDefaultSerializers() {
		if (!props.containsKey("key.serializer")){
			if(messageAH.isString()){
				props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
				trace.log(TraceLevel.INFO, "Adding unspecified property key.serializer=org.apache.kafka.common.serialization.StringSerializer" );
			}
			else{
				props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
				trace.log(TraceLevel.INFO, "Adding unspecified property key.serializer=org.apache.kafka.common.serialization.ByteArraySerializer" );
			}
		}
		
		if (!props.containsKey("value.serializer")){
			if(messageAH.isString()){
				props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
				trace.log(TraceLevel.INFO, "Adding unspecified property value.serializer=org.apache.kafka.common.serialization.StringSerializer" );
			}
			else{
				props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
				trace.log(TraceLevel.INFO, "Adding unspecified property value.serializer=org.apache.kafka.common.serialization.ByteArraySerializer" );
			}
		}
		
	}

}

class ProducerStringHelper extends KafkaProducerClient{

	private KafkaProducer<String, String> producer = null;
	
	public ProducerStringHelper(AttributeHelper topicAH, AttributeHelper keyAH,
			AttributeHelper messageAH, Properties props) {
		super(topicAH, keyAH, messageAH, props);
		producer = new KafkaProducer<String, String>(props);
		trace.log(TraceLevel.INFO, "Creating producer of type KafkaProducer<String,String>" );
	}



	@Override
	void send(Tuple tuple) throws Exception {
		String topic = topicAH.getString(tuple);
		String message = messageAH.getString(tuple);
		String key = keyAH.getString(tuple);

		producer.send(new ProducerRecord<String, String>(topic ,key, message));
	}

	@Override
	void send(Tuple tuple, List<String> topics) throws Exception {
		String message = messageAH.getString(tuple);
		String key = keyAH.getString(tuple);

		for(String topic : topics) {
			producer.send(new ProducerRecord<String, String>(topic,key, message));
		}

	}
	
	@Override
	void shutdown(){
		if (producer != null)
			producer.close();
	}
}


class ProducerByteHelper extends KafkaProducerClient{
	private KafkaProducer<byte[],byte[]> producer = null;
	
	public ProducerByteHelper(AttributeHelper topicAH, AttributeHelper keyAH,
			AttributeHelper messageAH, Properties props) {
		super(topicAH, keyAH, messageAH, props);
		producer = new KafkaProducer<byte[],byte[]>(props);
		trace.log(TraceLevel.INFO, "Creating producer of type KafkaProducer<Byte,Byte>" );
	}


	@Override
	void send(Tuple tuple) throws Exception {
		String topic = topicAH.getString(tuple);
		byte [] message = messageAH.getBytes(tuple);
		byte [] key = keyAH.getBytes(tuple);

		producer.send(new ProducerRecord<byte[],byte[]>(topic ,key, message));
	}

	@Override
	void send(Tuple tuple, List<String> topics) throws Exception {
		byte [] message = messageAH.getBytes(tuple);
		byte [] key = keyAH.getBytes(tuple);

		for(String topic : topics) {
			producer.send(new ProducerRecord<byte[],byte[]>(topic,key, message));
		}

	}
	
	@Override
	void shutdown(){
		if (producer != null)
			producer.close();
	}

}


