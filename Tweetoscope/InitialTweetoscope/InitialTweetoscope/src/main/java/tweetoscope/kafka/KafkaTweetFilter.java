package tweetoscope.kafka;

import com.twitter.clientlib.model.Tweet;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.OffsetDateTime;
import java.util.Properties;

import java.util.Collections;

import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.apache.kafka.clients.producer.ProducerRecord;
import tweetoscope.tweetsFilter.RecentTweetFilter;
import tweetoscope.tweetsFilter.TweetFilter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.time.OffsetDateTime;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonSerializer;
import java.time.format.DateTimeFormatter;

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

    public static void main(String[] args) {
        new KafkaTweetFilter();
    }

    KafkaTweetFilter() {
        try (KafkaConsumer<Void, String> kafkaConsumer = new KafkaConsumer<>(configureKafkaConsumer())) {
            // subscribes to the 'test' topic
            kafkaConsumer.subscribe(Collections.singletonList(INPUT_TOPIC_NAME));

            // reads from the topic
            try {
                Duration timeout = Duration.ofMillis(1000);
                ConsumerRecords<Void, String> records = null;
                while (true) { // I'm a machine, I can work forever :-)
                    // System.out.println("Test");
                    records = kafkaConsumer.poll(timeout);
                    for (ConsumerRecord<Void, String> record : records) {
                        // PART 1: Consume raw tweets from raw-tweets
                        String record_value = record.value();
                        Tweet tweet = gson.fromJson(record_value, Tweet.class);


                        // PART 2: Make filter // NOTE AMELIORATION: A METTRE EN DEHORS DE LA BOUCLE POUR EVITER DE RECREER LE FILTER A CHAQUE FOIS
                        // for now, we will only work with recent filter
                        KafkaTweetFilter.RecentTweetFilter filter = new KafkaTweetFilter.RecentTweetFilter(24);

                        // Apply filter:
                        if (filter.match(tweet)) {
                            // PART 3: If the tweet matches the filter, produce to filtered-tweets topic:
                            // NOTE AMELIORATION: A METTRE EN DEHORS DE LA BOUCLE POUR EVITER DE RECREER LE PRODUCER A CHAQUE FOIS
                            try (KafkaProducer<Void, String> kafkaProducer = new KafkaProducer<>(configureKafkaProducer())) {
                                String tweetJson = gson.toJson(tweet);
                                kafkaProducer.send(new ProducerRecord<Void, String>(OUTPUT_TOPIC_NAME, null, tweetJson));
                            }
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

    private Properties configureKafkaConsumer() {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.VoidDeserializer");
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // from beginning
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "myOwnPrivateJavaGroup");
        return consumerProperties;
    }

    private Properties configureKafkaProducer() {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.VoidSerializer");
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        return producerProperties;
    }

    static class RecentTweetFilter {
        protected long maxAge;

        public RecentTweetFilter(long maxAge) {
            this.maxAge = maxAge;
        }

        protected boolean match(Tweet tweet) {
            OffsetDateTime tweetDate = tweet.getCreatedAt();
            OffsetDateTime now = OffsetDateTime.now();
            long ageHours = Duration.between(tweetDate, now).toHours();

            return ageHours <= this.maxAge;
        }
    }
}