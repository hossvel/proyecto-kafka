package com.devhoss.kafka.assign.seek;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.LoggerFactory;

public class AssignSeekConsumer {

	// Create a Logger
	public static final org.slf4j.Logger log = LoggerFactory.getLogger(AssignSeekConsumer.class);

	public static void main(String[] args) {

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("group.id", "devhoss-group");
		props.setProperty("enable.auto.commit", "true");
		props.setProperty("auto.commit.interval.ms", "1000");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);) {
			//consumer.subscribe(Arrays.asList("testtopicreplication3"));
			TopicPartition topicPartition = new TopicPartition("testtopicreplication3", 1);// es la particion que vamos a leer
			consumer.assign(Arrays.asList(topicPartition));
			consumer.seek(topicPartition, 76830);// la posicion de la particion de donde se desea leer


			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

				for (ConsumerRecord<String, String> record : records) 
					log.info("partition50 = {} ,offset = {}, key = {}, value ={}",record.partition(), record.offset(),record.key(),record.value());
				//System.out.println( "*******************offset = " + record.offset() + "key = " + record.key() + "value = " + record.value());


			}
		}

	}
}
