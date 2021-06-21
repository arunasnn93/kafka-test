package com.arun.test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerTestKafkaThread {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		new ConsumerTestKafkaThread().run();
	}
	
	private ConsumerTestKafkaThread() {
		
	}
	
	private void run() {
		Logger logger = LoggerFactory.getLogger(ConsumerTestKafkaThread.class);
		CountDownLatch latch = new CountDownLatch(1);
		Runnable consthread = new ConsumerThread(latch);
		Thread thread = new Thread(consthread);
		thread.start();
		
		//shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread( () -> {
			 logger.info("Caught Shoutdown hook");
			 ((ConsumerThread) consthread).shutDown();
			 try {
				latch.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			logger.info("Application has exited"); 
		}
		));
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("Application interrupted", e);
		}finally {
			logger.info("Application is closing");
		}
	}

	
	public class ConsumerThread implements Runnable{
		
		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;
		private Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
		
		public ConsumerThread(CountDownLatch latch) {
			this.latch = latch;
			
			Properties props = new Properties();
			props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
			props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-app");
			props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			
			
			consumer = new KafkaConsumer<String, String>(props);
			consumer.subscribe(Arrays.asList("first_topic"));
		}

		public void run() {
			try {
				while(true) {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
					for(ConsumerRecord<String, String> record : records) {
						logger.info("Key : "+record.key());
						logger.info("Value : "+record.value());
						logger.info("Partition : "+record.partition());
						logger.info("Offset : "+record.offset());
					}
				}
			} catch (WakeupException e) {
				logger.info("Received shutdown");
			}finally {
				consumer.close();
				latch.countDown();
			}
		}
		
		public void shutDown() {
			consumer.wakeup();
		}
		
	}
}

