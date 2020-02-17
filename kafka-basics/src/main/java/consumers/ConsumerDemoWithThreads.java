package consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {

    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }

    private ConsumerDemoWithThreads(){
    }

    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);


        String bootstrapServer = "localhost:9092";
        String groupId = "my-sixth-application";
        String topic = "first_topic";

        CountDownLatch latch = new CountDownLatch(1);

        //Create the consumer runnable
        logger.info("Creating consumer thread....");
        Runnable myConsumerRunnable = new ConsumerRunnable(
                bootstrapServer,
                groupId,
                topic,
                latch
        );

        //Starts the thread
        Thread mThread = new Thread(myConsumerRunnable);
        mThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook....");
            ((ConsumerRunnable)myConsumerRunnable).shutdown();
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        }   finally {
            logger.info("Application is closing...");
        }

    }

    public class ConsumerRunnable implements Runnable{

        private CountDownLatch latch;
        private KafkaConsumer consumer;
        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);


        public ConsumerRunnable(String bootstrapServers, String groupId, String topic, CountDownLatch latch){
            this.latch = latch;

            //create Consumer properties
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers); //Default bootstrap server
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //read from the very beginning other values: [latest, none]

            //Create the consumer
            consumer = new KafkaConsumer<>(properties);

            //Subscribe to topic(s)
            consumer.subscribe(Collections.singleton(topic)); //Can also accept a list representing more topics
        }

        @Override
        public void run() {
            try{
                //poll new data
                while(true){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    records.forEach(r -> {
                        logger.info("Key : " + r.key() + " - Value : " + r.value());
                        logger.info("Partition : " + r.partition() + " - Offset : " + r.offset());
                    });
                }
            }catch (WakeupException ex){
                logger.info("Received shutdown signal!");
            }finally {
                consumer.close();
                latch.countDown(); //tells the main code that the consumer is done
            }
        }

        public void shutdown(){
            //this is called to interrupt poll() (Will throw WakeUpException)
            consumer.wakeup();

        }
    }

}
