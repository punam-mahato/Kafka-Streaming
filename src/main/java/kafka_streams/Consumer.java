package kafka_streams;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.Arrays;

public class Consumer {


    public static void main(String[] args) throws IOException {

     if(args.length < 2){
         System.out.println("Usage: consumer <topic> <groupname>");
         return;
      }

      String topic = args[0].toString();
      String group = args[1].toString();

      Properties props = new Properties();
      props.put("bootstrap.servers", "localhost:9092");
      props.put("zookeeper.connect", "localhost:2181");
      props.put("group.id", group);
      props.put("enable.auto.commit", "true");
      props.put("auto.commit.interval.ms", "1000");
      props.put("session.timeout.ms", "30000");
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      props.put("value.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
      props.put("auto.offset.reset", "earliest");

      KafkaConsumer<String, Long> consumer = new KafkaConsumer<String, Long>(props);
      consumer.subscribe(Arrays.asList(topic));
      System.out.println("Subscribed to topic " + topic);   
   

      while (true) {
         ConsumerRecords<String, Long> records = consumer.poll(100);
            for (ConsumerRecord<String, Long> record : records){
              System.out.println("region: "+ record.key() +"            clicks: "+record.value());
            }

      }
  }
}
