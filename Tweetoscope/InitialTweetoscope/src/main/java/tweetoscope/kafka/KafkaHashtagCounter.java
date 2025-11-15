package tweetoscope.kafka;



import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

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
import com.twitter.twittertext.Extractor;

public class KafkaHashtagCounter {

    private static final String INPUT_TOPIC_NAME = "hashtags";
    private static final String OUTPUT_TOPIC_NAME = "leaderboards";

    private static final int NB_LEADERS = 5;

    private final Gson gson = new GsonBuilder()
            .registerTypeAdapter(OffsetDateTime.class,
                    (JsonSerializer<OffsetDateTime>) (src, typeOfSrc, context) ->
                            context.serialize(src.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)))
            .registerTypeAdapter(OffsetDateTime.class,
                    (JsonDeserializer<OffsetDateTime>) (json, typeOfT, context) ->
                            OffsetDateTime.parse(json.getAsString(), DateTimeFormatter.ISO_OFFSET_DATE_TIME))
            .create();

    final Extractor twitterTextExtractor = new Extractor();

    private final Map<String, Integer> hashtagOccurrenceMap = new HashMap<>(); // AJOUT
    private Map<String, Integer> previousLeaderMap = null; // AJOUT

    public static void main(String[] args) {
        new KafkaHashtagCounter();
    }

    KafkaHashtagCounter() {
        System.out.println("[Counter] Starting...");
        try (KafkaProducer<Void, String> kafkaProducer = new KafkaProducer<>(configureKafkaProducer())) {
            System.out.println("[Counter] Producer connected to Kafka");
            try (KafkaConsumer<Void, String> kafkaConsumer = new KafkaConsumer<>(configureKafkaConsumer())) {
                // subscribes to the input topic
                System.out.println("[Counter] Consumer created, subscribing to " + INPUT_TOPIC_NAME);
                kafkaConsumer.subscribe(Collections.singletonList(INPUT_TOPIC_NAME));

                // reads from the topic
                try {
                    Duration timeout = Duration.ofMillis(1000);
                    ConsumerRecords<Void, String> records = null;
                    int pollCount = 0;
                    while (true) {
                        records = kafkaConsumer.poll(timeout);
                        if (++pollCount % 10 == 0) {
                            System.out.println("[Counter] Poll #" + pollCount + ": " + records.count() + " records, map size: " + hashtagOccurrenceMap.size());
                        }
                        for (ConsumerRecord<Void, String> record : records) {
                            String raw = record.value();
                            if (raw == null || raw.isBlank()) continue;

                            String tag = raw.trim();
                            if (!tag.startsWith("#")) tag = "#" + tag;

                            synchronized (hashtagOccurrenceMap) {
                                hashtagOccurrenceMap.merge(tag, 1, Integer::sum);

                                Map<String, Integer> topHashtagsMap =
                                        hashtagOccurrenceMap.entrySet().stream()
                                                .sorted(Map.Entry.<String,Integer>comparingByValue(Comparator.reverseOrder()))
                                                .limit(NB_LEADERS)
                                                .collect(Collectors.toMap(
                                                        Map.Entry::getKey,
                                                        Map.Entry::getValue,
                                                        (a,b) -> a,
                                                        LinkedHashMap::new));

                                // Publie seulement si le leaderboard a changé
                                if (previousLeaderMap == null || !previousLeaderMap.equals(topHashtagsMap)) {
                                    String payload = gson.toJson(topHashtagsMap);
                                    kafkaProducer.send(new ProducerRecord<>(OUTPUT_TOPIC_NAME, null, payload));
                                    System.out.println("[Counter] Published leaderboard: " + payload);
                                    previousLeaderMap = topHashtagsMap;
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("something went wrong... " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    kafkaConsumer.close();
                }


            }
        }
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
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // from beginning
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "myOwnPrivateJavaGroup");
        return consumerProperties;
    }
}