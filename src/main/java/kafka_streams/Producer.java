package kafka_streams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;


public class Producer {

    public static void main(String[] args) throws IOException {
        
        KafkaProducer<String, Long> producer1;
        Properties props1 = new Properties();
        props1.put("bootstrap.servers", "localhost:9092");        
        props1.put("client.id", "Producer.1");
        props1.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");        
        props1.put("value.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        producer1 = new KafkaProducer<>(props1);

        KafkaProducer<String, String> producer2;
        Properties props2 = new Properties();
        props2.put("bootstrap.servers", "localhost:9092");        
        props2.put("client.id", "DemoProducer2");
        props2.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");        
        props2.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer2 = new KafkaProducer<>(props2);
        String[] states = {"California", "Alabama", "Arkansas", "Arizona", "Alaska", "Colorado", "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming" };


        try {

            for (int i = 0; i < 100000; i++) {
                Random rn = new Random();

                int rnd =	rn.nextInt(500);                
                String user= (String)("user"+Integer.toString(i));

                long range = 1000L;                
                long clicks = (long)(rn.nextDouble()*range);                

                ProducerRecord record = new ProducerRecord<>("user-clicks", user, clicks);
                producer1.send(record);

                int rand = rn.nextInt(50);
                String location =states[rand];

                
                ProducerRecord rec = new ProducerRecord<>("user-location", user, location);
                producer2.send(rec);

                if(clicks%7==0){System.out.println("\n ----- Writing records to topics ----------  \n" );}

            }
        }  finally {
            
            producer1.close();
            producer2.close();
        }

    }
}
