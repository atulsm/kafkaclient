package com.atul.kafka;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
 
public class KafkaProducer101 {
    private static final String TOPIC = "eb-cef";
    private static String msg = "CEF:0|Security|threatmanager2|1.0|100|worm successfully stopped|10|src=10.0.0.1 dst=2.1.2.2 spt=1232"; 


	public static void main(String[] args) { 
        Properties props = new Properties();
        //props.put("bootstrap.servers", "164.99.175.163:9092");
        props.put("bootstrap.servers", "idcdvstl200.stldom100.lab:9092");
        
       // props.put("request.required.acks", "1");
        //props.put("producer.type","async");

 
        Serializer<String> keySerializer =  new StringSerializer();
		Serializer<String> valueSerializer =  new StringSerializer();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props, keySerializer, valueSerializer);
        
        ProducerRecord<String, String> data = new ProducerRecord<String, String>(TOPIC, msg);
        producer.send(data);
        
        producer.close();
    }
	
}
