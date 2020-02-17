package producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

public class ProducerCallBacksWithKeys {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerCallBacksWithKeys.class);

        String bootstrapServer = "localhost:9092";

        //create Producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer); //Default bootstrap server
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        IntStream.range(0, 10).forEach( i -> {
            //send data - async

            String topic = "first_topic";
            String key = "Key_" + i;
            String value = "hello by code - " + i;


            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            //The SAME KEY always go to SAME PARTITION
            logger.info("Key : " + key);

            try {
                producer.send(record, (recordMetadata, e) -> {
                    //execute everytime a record is sucessfully sent or an exception is logger
                    if(Objects.isNull(e)){
                        logger.info("Received new metadata." +
                                " \n Topic : " + recordMetadata.topic() +
                                " \n Partition : " + recordMetadata.partition() +
                                " \n Offset : " + recordMetadata.offset() +
                                " \n Timestamp : " + recordMetadata.timestamp());
                    }else{
                        logger.error("Error while producing", e);
                    }

                }).get(); //just making it synchronous to test


            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });

        //flushing data
        producer.flush();
        //flush the producer
        producer.close();
    }
}
