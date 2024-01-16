package dev.suga.demos.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {
    public static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());


    public static void main(String[] args) throws IOException {
        RestHighLevelClient openSearchClient = createOpenSearchClient();
        String index = "wikimedia";
        //create kafka consumer
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();

        // get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        // adding shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                log.info("detected a shutdown, let's exit by calling consumer.wakeup()....");
                kafkaConsumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try(openSearchClient; kafkaConsumer) {
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT);
            if( !indexExists ){
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The wikimedia has been created!");
            } else {
                log.info("The wikimedia index already exist");
            }

            kafkaConsumer.subscribe(Collections.singleton("wikimedia.recentchanges"));

            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(3000));
                int recordCount = records.count();
                log.info("Received " + recordCount + " record(s)");

                BulkRequest bulkRequest = new BulkRequest();
                for(ConsumerRecord<String, String> record : records) {
                    try {
                        String id = extractId(record.value());

                        IndexRequest indexRequest = new IndexRequest(index)
                                .source(record.value(), XContentType.JSON).id(id);
                        //IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT );
                        bulkRequest.add(indexRequest);
                       // log.info("Inserted 1 document into OpenSearch " + response.getId());
                    }catch (Exception e) {

                    }

                }
                if(bulkRequest.numberOfActions() > 0){
                    BulkResponse bulkItemResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted " + bulkItemResponse.getItems().length + " record(s).");
                    try {
                        Thread.sleep(1000);
                    }catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }
        }




    }

    private static String extractId(String json) {
        return JsonParser.parseString(json).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
    }

    private static RestHighLevelClient createOpenSearchClient() {
        // Configura las direcciones de los nodos de OpenSearch
        String connString = "http://localhost:9200";
        URI connURI = URI.create(connString);
        // Configura el RestClient
        RestClientBuilder builder = RestClient.builder(new HttpHost(connURI.getHost(), connURI.getPort() ));

        // Crea el RestHighLevelClient
        RestHighLevelClient client = new RestHighLevelClient(builder);

        return client;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(){
        String groupId = "consumer=opensearch-demo";
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");

        // create consumer config
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // create consumer
        return new KafkaConsumer<>(properties);
    }
}
