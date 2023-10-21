package com.devhoss.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerKafka {

	public static void main(String[] args) {
		long inicio = System.currentTimeMillis();
		
		Properties props=new Properties();
		props.put("bootstrap.servers","localhost:9093");
		props.put("acks","1");// SE NESESITA QUE POR LO MENOS UN NODO NOS RESPONDA QUE RECIBIO
		props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
		props.put("linger.ms","5");
		try(Producer<String, String>producer=new KafkaProducer<>(props);) {
			for(int i= 0; i< 10000;i++) {
				producer.send(new ProducerRecord<>("testtopicreplication3",String.valueOf(i),"Mensaje - " + (i + 1)));
			}
			producer.flush();
		}
		
		System.out.println("Tiempo de procesamiento: "  + (System.currentTimeMillis() - inicio) + " ms");
	}



}
