package com.atul.kafka;

import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.common.util.concurrent.RateLimiter;

public class ThrottledKafkaEventProducer extends Thread{
	
	private String[] args;
	private static final int CLIENT_COUNT=1;
	public static RateLimiter limiter;
	private static int recordSize=1000;
	
	public ThrottledKafkaEventProducer(String args[]){
		this.args = args;
	}
	
	public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("USAGE: java " + ThrottledKafkaEventProducer.class.getName() + " url num_records EPS");
            System.exit(1);
        }
        
        int eps = Integer.parseInt(args[2]);
        limiter = RateLimiter.create(eps);
        
        int numRecords = Integer.parseInt(args[1]);
        long start = System.currentTimeMillis();

        ThrottledKafkaEventProducer ins[] = new  ThrottledKafkaEventProducer[CLIENT_COUNT];
        for(int i =0;i<ins.length;i++){
        	ins[i] = new ThrottledKafkaEventProducer(args);
        	ins[i].start();
        }

        for(int i =0;i<ins.length;i++){
           	ins[i].join();
        }
                       
        long ellapsed = System.currentTimeMillis() - start;
        double msgsSec = 1000.0 * numRecords / (double) ellapsed;
        double mbSec = msgsSec * (recordSize + Records.LOG_OVERHEAD) / (1024.0 * 1024.0);
        System.out.printf("%d records sent in %d ms ms. %.2f records per second (%.2f mb/sec).", numRecords, ellapsed, msgsSec, mbSec);
	}
	
    public void run() {
        String url = args[0];
        int numRecords = Integer.parseInt(args[1])/CLIENT_COUNT;
        Properties props = new Properties();
        props.setProperty(ProducerConfig.ACKS_CONFIG, "0");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "8196");
        props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, url);
        KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props,new StringSerializer(), new ByteArraySerializer());
        //KafkaProducer<Object, byte[]> producer = new KafkaProducer<Object, byte[]>();
        
        Callback callback = new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e != null)
                    e.printStackTrace();
            }
        };
        
        long maxLatency = -1L;
        long totalLatency = 0;
        int reportingInterval = 1000000;
        for (int i = 0; i < numRecords; i++) {
        	limiter.acquire();
            long sendStart = System.currentTimeMillis();
            byte[] payload = getPayLoad();        
            ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>("loadtest", payload);
            producer.send(record, callback);
            //producer.send(record);
            long sendEllapsed = System.currentTimeMillis() - sendStart;
            maxLatency = Math.max(maxLatency, sendEllapsed);
            totalLatency += sendEllapsed;
            if (i % reportingInterval == 0) {
                System.out.printf("%d  max latency = %d ms, avg latency = %.5f\n",
                                  i,
                                  maxLatency,
                                  (totalLatency / (double) reportingInterval));
                totalLatency = 0L;
                maxLatency = -1L;
            }
            
            if(i%100000==0){
            	System.out.println(new Date()  + "  Send " + i + " records ");
            }
        }
        producer.close();
    }
    
    private static byte[] getPayLoad(){
    	String evt = "CEF:0|Security|threatmanager2|1.0|100|worm successfully stopped|10|src=10.0.0.1 dst=2.1.2.2 spt=1232";
    	byte[] ret =  evt.getBytes();
    	recordSize = ret.length;
    	return ret;
    }
}
