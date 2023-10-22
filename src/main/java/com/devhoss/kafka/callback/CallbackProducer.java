package com.devhoss.kafka.callback;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.LoggerFactory;


public class CallbackProducer {

	public static final org.slf4j.Logger log = LoggerFactory.getLogger(CallbackProducer.class);
	
	public static void main(String[] args) {
		long inicio = System.currentTimeMillis();

		Properties props=new Properties();
		props.put("bootstrap.servers","localhost:9093");
		props.put("acks","1");// SE NESESITA QUE POR LO MENOS UN NODO NOS RESPONDA QUE RECIBIO
		props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
		props.put("linger.ms","5");

		try(Producer<String, String>producer=new KafkaProducer<>(props);) {

			for(int i= 0; i < 10000;i++) {
				//producer.send(new ProducerRecord<>("testtopicreplication3",String.valueOf(i),"Mensaje - " + (i + 1)));

				producer.send(new ProducerRecord<>("testtopicreplication3",String.valueOf(i),"Mensaje - " + (i + 1)), new Callback() {

					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if(exception!=null) {
							log.error("Error sending the message" ,exception);
						}
						log.info("Partition = {} Offset ={}", metadata.partition(),metadata.offset());
					}
				});

			}
			producer.flush();
		}

		System.out.println("Tiempo de procesamiento: "  + (System.currentTimeMillis() - inicio) + " ms");
	}


}
