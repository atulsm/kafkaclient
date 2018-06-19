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
    private static String msg = "CEF:0|Security|threatmanager2|1.0|100|worm successfully stopped|10|src=10.0.0.1 dst=2.1.2.2 spt=1232 "; 
    //private static String msg = "CEF:0|Atul|Test|Windows 10|SelfServicePlugIn:1|Test Event Name|Low| eventId=5762 externalId=1 cat=Application deviceSeverity=Information sourceServiceName=SelfServicePlugIn oldFileHash=UTF-8| cs2=None cs3=SelfServicePlugIn cs2Label=EventlogCategory cs3Label=EventSource ahost=ind-l-cmanohar.microfocus.com agt=164.99.135.69 agentZoneURI=/All Zones/ArcSight System/Public Address Space Zones/ARIN/164.0.0.0-169.253.255.255 (ARIN) amac=A4-4C-C8-9E-67-AE av=7.8.0.8070.0 atz=Asia/Calcutta at=winc dvchost=IND-L-CManohar.microfocus.com dvc=137.65.222.221 deviceZoneURI=/All Zones/ArcSight System/Public Address Space Zones/ARIN/128.0.0.0-140.255.255.255 (ARIN) dtz=Asia/Calcutta _cefVer=0.1 ad.EventRecordID=43148 ";
    
	public static void main(String[] args) { 
        Properties props = new Properties();
        //props.put("bootstrap.servers", "164.99.175.163:9092");
        props.put("bootstrap.servers", "idcdvstl200.stldom100.lab:9092");
 
        Serializer<String> keySerializer =  new StringSerializer();
		Serializer<String> valueSerializer =  new StringSerializer();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props, keySerializer, valueSerializer);
        
        msg = msg + " rt=" + System.currentTimeMillis();
        ProducerRecord<String, String> data = new ProducerRecord<String, String>(TOPIC, msg);
        producer.send(data);
        producer.close();
    }
	
}
