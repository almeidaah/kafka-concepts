package almeida.fernando.kafka.elasticsearch;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {


    public static RestHighLevelClient createClient(){

        String hostname = "kafka-course-667112198.eu-west-1.bonsaisearch.net";
        String username = "10k980ili2";
        String password = "fsp2c8z4i5";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        HttpHost https = new HttpHost(hostname, 443, "https");

        RestClientBuilder builder = RestClient.builder(https)
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        RestHighLevelClient client = new RestHighLevelClient(builder);

        return client;
    }


    public static void main(String[] args) throws IOException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        RestHighLevelClient client = createClient();
        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

        //poll new data
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> r : records){

                //kafka id
                //String id = r.topic() + "_" + r.partition() + "_" + r.offset();

                //twitter id
                String id = extractIdFromTweet(r.value());

                //Insert data into elasticSearch
                IndexRequest indexRequest = new IndexRequest("twitter", "_doc", id) //Idempotent
                                                .source(r.value(), XContentType.JSON);
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);


                logger.info("ID : {}", indexResponse.getId());
                Thread.sleep(1000);

            }
        }

        //client.close();


    }


    public static KafkaConsumer<String, String> createConsumer(String topic){
        String bootstrapServer = "localhost:9092";
        String groupId = "kafka-demo-elasticsearch";

        //create Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer); //Default bootstrap server
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //read from the very beginning other values: [latest, none]

        //Create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //Subscribe to topic(s)
        consumer.subscribe(Collections.singleton(topic)); //Can also accept a list representing more topics

        return consumer;
    }

    private static JsonParser jsonParser = new JsonParser();

    private static String extractIdFromTweet(String tweetJson) {
        //gson extract
        String id_str = jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
        return id_str;
    }
}
