package com.devhoss.kafka.transaccional;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransaccionalProducer {

	public static final Logger log= LoggerFactory.getLogger(TransaccionalProducer.class);

	public static void main(String[] args) {

		Properties props=new Properties();
		props.put("bootstrap.servers","localhost:9092");
		props.put("acks","all");
		props.put("compression.type","gzip");
		props.put("linger.ms","1");
		props.put("batch.size","32384");
		props.put("transactional.id","devhoss-producer-id");
		props.put("buffer.memory","33554432");
		props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

		try(Producer<String, String> producer = new KafkaProducer<>(props)) {
			try{
				producer.initTransactions();
				producer.beginTransaction();
				for(int i= 0;i < 100000; i++) {					
					producer.send(new ProducerRecord<>("testtopicreplication3",String.valueOf(i + 1),"Mensaje - " + (i + 1)));
					if(i== 50000) {
						throw new Exception("Random exception");
					}
				}
				producer.commitTransaction();
				producer.flush();
			}catch(Exception e) {
				producer.abortTransaction();
				log.error("Error processing messages",e);
			}

		}
	}
}
