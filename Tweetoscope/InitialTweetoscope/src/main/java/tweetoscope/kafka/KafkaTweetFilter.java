package tweetoscope.kafka;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonSerializer;
import com.twitter.clientlib.model.Tweet;

import tweetoscope.tweetsFilter.EmptyTweetFilter;
import tweetoscope.tweetsFilter.LangTweetFilter;
import tweetoscope.tweetsFilter.RecentTweetFilter;
import tweetoscope.tweetsFilter.TweetFilter;

public class KafkaTweetFilter {

    private static final String INPUT_TOPIC_NAME = "raw-tweets";
    private static final String OUTPUT_TOPIC_NAME = "filtered-tweets";

    private final Gson gson = new GsonBuilder()
            .registerTypeAdapter(OffsetDateTime.class,
                    (JsonSerializer<OffsetDateTime>) (src, typeOfSrc, context) ->
                            context.serialize(src.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)))
            .registerTypeAdapter(OffsetDateTime.class,
                    (JsonDeserializer<OffsetDateTime>) (json, typeOfT, context) ->
                            OffsetDateTime.parse(json.getAsString(), DateTimeFormatter.ISO_OFFSET_DATE_TIME))
            .create();

    private static Object filter = new EmptyTweetFilter();

    public static void main(String[] args) {
        System.out.println("Parsing parameters from command line");
        if (args.length == 0) {
            long maxAge = 24;
            filter = new RecentTweetFilter(maxAge);
            System.out.println("Starting Filtering Tweets using recent filter");
            new KafkaTweetFilter();
        } else {
            switch (args[0]) {
                case "recent":
                    long maxAge = 24;
                    filter = new RecentTweetFilter(maxAge);
                    System.out.println("Starting Filtering Tweets using recent filter");
                    new KafkaTweetFilter();
                    break;
                case "lang":
                    String language = args.length > 1 ? args[1] : "en";
                    filter = new LangTweetFilter(language);
                    System.out.println("Starting Filtering Tweets using language filter");
                    new KafkaTweetFilter();
                    break;
                case "empty":
                default:
                    filter = new EmptyTweetFilter();
                    System.out.println("Starting Filtering Tweets using no empty tweet filter");
                    new KafkaTweetFilter();
                    break;
            }
        }
    }


    KafkaTweetFilter() {
        try (KafkaProducer<Void, String> kafkaProducer = new KafkaProducer<>(configureKafkaProducer())) {
            try (KafkaConsumer<Void, String> kafkaConsumer = new KafkaConsumer<>(configureKafkaConsumer())) {
                // subscribes to the 'test' topic
                kafkaConsumer.subscribe(Collections.singletonList(INPUT_TOPIC_NAME));

                // reads from the topic
                try {
                    Duration timeout = Duration.ofMillis(1000);
                    ConsumerRecords<Void, String> records = null;

                    // Deprecated:
                    // Uncomment the desired filter:
                    // RecentTweetFilter filter = new RecentTweetFilter(24);
                    // LangTweetFilter filter = new LangTweetFilter("en");
                    // EmptyTweetFilter filter = new EmptyTweetFilter();

                    while (true) { // I'm a machine, I can work forever :-)
                        // System.out.println("Test");
                        records = kafkaConsumer.poll(timeout);
                        for (ConsumerRecord<Void, String> record : records) {
                            try {
                                // PART 1: Consume raw tweets from raw-tweets
                                String record_value = record.value();
                                if (record_value == null || record_value.isEmpty()) {
                                    continue;
                                }

                                Tweet tweet = gson.fromJson(record_value, Tweet.class);
                                
                                if (tweet == null) {
                                    System.err.println("Warning: Deserialized tweet is null, skipping...");
                                    continue;
                                }

                                // PART 2: Apply filter
                                boolean matches = ((TweetFilter) filter).isMatching(tweet);

                                if (matches) {
                                    // PART 3: If the tweet matches the filter, produce to filtered-tweets topic
                                    String tweetJson = gson.toJson(tweet);
                                    kafkaProducer.send(new ProducerRecord<>(OUTPUT_TOPIC_NAME, null, tweetJson));
                                    System.out.println("Filtered tweet sent to partition with id: " + tweet.getId());
                                }
                            } catch (Exception recordException) {
                                System.err.println("Error processing record: " + recordException.getMessage());
                                recordException.printStackTrace();
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("something went wrong... " + e.getMessage());
                } finally {
                    kafkaConsumer.close();
                }
            }
        }
    }

    private Properties configureKafkaConsumer() {
        Properties consumerProperties = new Properties();
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            bootstrapServers = "kafka:9092";
        }
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.VoidDeserializer");
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaTweetFilter-Group");
        return consumerProperties;
    }

    private Properties configureKafkaProducer() {
        Properties producerProperties = new Properties();
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            bootstrapServers = "kafka:9092";
        }
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.VoidSerializer");
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        return producerProperties;
    }


    // Same for UserAccountCreationDate.



}