package com.devhoss.kafka.multihilo;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaThreadConsumer extends Thread {
	private final KafkaConsumer<String, String>consumer;
	private final AtomicBoolean closed = new AtomicBoolean(false);
	public static final Logger log= LoggerFactory.getLogger(KafkaThreadConsumer.class);

	public KafkaThreadConsumer(KafkaConsumer<String, String>consumer) {
		this.consumer=consumer;
	}
	@Override
	public void run() {
		try{
			consumer.subscribe(Arrays.asList("testtopicreplication3")); while(!closed.get()) {
				ConsumerRecords<String, String>records = consumer.poll(Duration.ofMillis(10000));
				for(ConsumerRecord<String, String> record : records) {
					log.debug("partition = {} ,offset = {}, key = {}, value ={}",record.partition(), record.offset(),record.key(),record.value());
					if(Integer.parseInt(record.key())%1000 == 0)
						log.info("partition = {} ,offset = {}, key = {}, value ={}",record.partition(), record.offset(),record.key(),record.value());

				}
			}
		}catch(WakeupException e) {
			if(!closed.get())
				throw e;
		}finally{
			consumer.close();
		}
	}
	public void shutdown() {
		closed.set(true);
		consumer.wakeup();
	}
}
