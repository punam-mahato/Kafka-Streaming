package kafka_streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.StreamsConfig;
import java.io.IOException;
import java.util.Properties;

public class WindowedJoin {

    static public class RegionClicks {
        public long clicks;
        public String region;
    }


    public static void main(String[] args) throws IOException{
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka.streams.1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-windowedjoin");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KStreamBuilder builder = new KStreamBuilder();


        KStream<String, Long> userclicks = builder.stream(Serdes.String(), Serdes.Long(), "user-clicks");

        KStream<String, String> userregion = builder.stream(Serdes.String(), Serdes.String(), "user-location");


        //<V1,R> KStream<K,R> join(KStream<K,V1> otherStream, ValueJoiner<V,V1,R> joiner, JoinWindows windows, Serde<K> keySerde, Serde<V> thisValueSerde, Serde<V1> otherValueSerde)
        //Combine element values of this stream with another KStream's elements of the same key using windowed Inner Join.
        KStream<String, Long> regionclicks = userclicks.join(userregion, new ValueJoiner<Long, String, RegionClicks>(){
            @Override
            public RegionClicks apply(Long clicks, String region){
                RegionClicks rc = new RegionClicks();
                if (region != null){rc.region= region;}
                else rc.region= "UNKNOWN";
                rc.clicks=clicks;
                if(clicks%25==0){System.out.println("\n \n ----- Performing windowed join ----------");}                
                return rc;
            }
            }, JoinWindows.of("clicks-summary").within(10*1000L), Serdes.String(), Serdes.Long() ,Serdes.String())


            //map(KeyValueMapper<K,V,KeyValue<K1,V1>> mapper)
            //R apply(K key, V value)  Map a record with the given key and value to a new value.
            .map(new KeyValueMapper<String, RegionClicks, KeyValue<String, Long>>(){
                @Override
                public KeyValue<String, Long> apply(String user, RegionClicks rclicks){
                    return new KeyValue<>(rclicks.region, rclicks.clicks); 
                }
            }); 

            regionclicks.to(Serdes.String(), Serdes.Long(), "outputTopic2");

            System.out.println("Performed join.");
  
        
        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
        

    }
}
