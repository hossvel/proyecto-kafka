package com.devhoss.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.LoggerFactory;

public class ConsumerKafka {

	// Create a Logger
	public static final org.slf4j.Logger log = LoggerFactory.getLogger(ConsumerKafka.class);

	public static void main(String[] args) {

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("group.id", "devhoss-group");
		props.setProperty("enable.auto.commit", "true");
		props.setProperty("auto.commit.interval.ms", "1000");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);) {
			consumer.subscribe(Arrays.asList("testtopicreplication3"));
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

				for (ConsumerRecord<String, String> record : records) 
					log.info("partition = {} ,offset = {}, key = {}, value ={}",record.partition(), record.offset(),record.key(),record.value());
					//System.out.println( "*******************offset = " + record.offset() + "key = " + record.key() + "value = " + record.value());
					
				
			}
		}

	}
}
