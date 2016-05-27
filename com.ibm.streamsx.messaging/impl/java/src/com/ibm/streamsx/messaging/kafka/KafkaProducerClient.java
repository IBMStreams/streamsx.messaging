package com.ibm.streamsx.messaging.kafka;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.logging.TraceLevel;

import java.util.Map;
//import java.util.Map.Entry;

public abstract class KafkaProducerClient extends KafkaBaseClient {
	static String TIMEOUT_EXCEPTION = "org.apache.kafka.common.errors.TimeoutException";
	
	public KafkaProducerClient(AttributeHelper topicAH, AttributeHelper keyAH,
			AttributeHelper messageAH, Properties props){
		super(topicAH, keyAH, messageAH, props);
		props = KafkaConfigUtilities.setDefaultSerializers(keyAH, messageAH, props);
	}

	abstract void send(Tuple tuple, List<String> topics) throws Exception;

	abstract void send(Tuple tuple) throws Exception;
	
	abstract void checkConnectionCount() throws NoKafkaBrokerConnectionsException;

}

class ProducerStringHelper extends KafkaProducerClient{

	private KafkaProducer<String, String> producer = null;
	
	public ProducerStringHelper(AttributeHelper topicAH, AttributeHelper keyAH,
			AttributeHelper messageAH, Properties props) {
		super(topicAH, keyAH, messageAH, props);
		producer = new KafkaProducer<String, String>(props);
		trace.log(TraceLevel.INFO, "Creating producer of type KafkaProducer\\<String,String\\>" );
	}



	@Override
	void send(Tuple tuple) throws Exception {
		String topic = topicAH.getString(tuple);
		String message = messageAH.getString(tuple);
		String key = keyAH.getString(tuple);
		
		producer.send(new ProducerRecord<String, String>(topic ,key, message));

//		if (meta == null){
//			System.out.println("Meta null!");
//		} else {
//			System.out.println("Meta not null: " + meta.toString());
//		}
		
		//	               new Callback() {
//            public void onCompletion(RecordMetadata metadata, Exception e) {
//                if(e != null){
//                    e.printStackTrace();
//                    if (e.getClass().getName().equalsIgnoreCase(TIMEOUT_EXCEPTION)){
//                    	System.out.println("Lost connection!");
//                    }
//                }
//            }
//        });
	}

	@Override
	void send(Tuple tuple, List<String> topics) throws Exception {
		String message = messageAH.getString(tuple);
		String key = keyAH.getString(tuple);

		for(String topic : topics) {
			producer.send(new ProducerRecord<String, String>(topic ,key, message), 
		               new Callback() {
	            public void onCompletion(RecordMetadata metadata, Exception e) {
	                if(e != null)
	                    e.printStackTrace();
	               // System.out.println("The offset of the record we just sent is: " + metadata.offset());
	            }
	        });
		}

	}
	
	@Override
	void shutdown(){
		if (producer != null)
			producer.close();
	}



	@Override
	void checkConnectionCount() throws NoKafkaBrokerConnectionsException {
		//producer.metrics().forEach((k,v) -> System.out.println("key: "+k+" value:"+v.value()));
		@SuppressWarnings("unchecked")
		Map<MetricName,KafkaMetric> metricsMap = (Map<MetricName, KafkaMetric>) producer.metrics();
		
		for (Map.Entry<MetricName,KafkaMetric> metric : metricsMap.entrySet()){
			if (metric.getKey().name().equals("connection-count")){
				if (metric.getValue().value() == 0){
					//System.out.println("0!!!! Name: " + metric.getValue().metricName());
					throw new NoKafkaBrokerConnectionsException();
				} else {
					//System.out.println("Not 0!");
				}
			}
		}
	}
}


class ProducerByteHelper extends KafkaProducerClient{
	private KafkaProducer<byte[],byte[]> producer = null;
	
	public ProducerByteHelper(AttributeHelper topicAH, AttributeHelper keyAH,
			AttributeHelper messageAH, Properties props) {
		super(topicAH, keyAH, messageAH, props);
		producer = new KafkaProducer<byte[],byte[]>(props);
		trace.log(TraceLevel.INFO, "Creating producer of type KafkaProducer\\<Byte,Byte\\>" );
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


	@Override
	void checkConnectionCount() throws NoKafkaBrokerConnectionsException {
		@SuppressWarnings("unchecked")
		Map<MetricName,KafkaMetric> metricsMap = (Map<MetricName, KafkaMetric>) producer.metrics();
		
		for (Map.Entry<MetricName,KafkaMetric> metric : metricsMap.entrySet()){
			if (metric.getKey().name().equals("connection-count")){
				if (metric.getValue().value() == 0){
					//System.out.println("0!!!! Name: " + metric.getValue().metricName());
					throw new NoKafkaBrokerConnectionsException();
				} else {
					//System.out.println("Not 0!");
				}
			}
		}
	}

}


