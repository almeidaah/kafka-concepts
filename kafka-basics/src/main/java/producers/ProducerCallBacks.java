package producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

public class ProducerCallBacks {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerCallBacks.class);

        String bootstrapServer = "localhost:9092";

        //create Producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer); //Default bootstrap server
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //send data - async
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic","hello by code");

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

        });

        //flushing data
        producer.flush();
        //flush the producer
        producer.close();
    }
}
