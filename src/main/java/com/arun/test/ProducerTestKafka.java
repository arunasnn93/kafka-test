package com.arun.test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerTestKafka {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		// TODO Auto-generated method stub
		
		final Logger logger = LoggerFactory.getLogger(ProducerTestKafka.class);
		
		Properties prop = new Properties();
		prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
		
		
		for(int i=0; i< 9; i++) {
			
			String key = "id_"+Integer.toString(i);
			
			ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first_topic", key, "hello-world"+ Integer.toString(i));
			producer.send(producerRecord, new Callback() {
				
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					// TODO Auto-generated method stub
					if(exception ==null) {
						logger.info("Received new metadata \n"
								+ "Topic : "+metadata.topic()
								+ "\n Partition :"+metadata.partition()
								+ "\n Offset : "+metadata.offset()); 
					}else {
						logger.error("Error while producing");
					}
				}
			}).get();
		}
		
		producer.flush();
		producer.close();
	}

}
