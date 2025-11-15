package tweetoscope.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Kafka consumer for leaderboard data - works in both GUI and headless modes
 */
public class KafkaLeaderboard {

    private static final String INPUT_TOPIC_NAME = "leaderboards";
    private static final int NB_LEADERS = 5;

    // Gson + type pour Map<String,Integer>
    private final Gson gson = new GsonBuilder().create();
    private final java.lang.reflect.Type mapType =
            new com.google.gson.reflect.TypeToken<Map<String, Integer>>() {}.getType();

    // For GUI mode (optional)
    private transient KafkaLeaderboardGUI guiFrame = null;
    private transient KafkaLeaderboardWebServer webServer = null;

    public static void main(String[] args) {
        String headless = System.getProperty("java.awt.headless", "false");
        KafkaLeaderboard app = new KafkaLeaderboard(NB_LEADERS, headless.equals("true"));
        app.startConsumerThread();
    }

    public KafkaLeaderboard(int nbLeaders, boolean headlessMode) {
        // Always start web server (accessible from outside container)
        try {
            this.webServer = new KafkaLeaderboardWebServer();
        } catch (Exception e) {
            System.err.println("WebServer initialization failed: " + e.getMessage());
        }
        
        // Optionally start GUI if not headless
        if (!headlessMode) {
            try {
                this.guiFrame = new KafkaLeaderboardGUI("Tweetoscope Leaderboard");
                this.guiFrame.setVisible(true);
            } catch (Exception e) {
                System.err.println("GUI initialization failed: " + e.getMessage());
            }
        }
    }

    // Legacy constructor for backward compatibility
    public KafkaLeaderboard(int nbLeaders) {
        this(nbLeaders, false);
    }

    private void startConsumerThread() {
        Thread t = new Thread(this::runConsumer, "leaderboard-consumer");
        t.setDaemon(true);
        t.start();
    }

    private void runConsumer() {
        try (KafkaConsumer<Void, String> kafkaConsumer = new KafkaConsumer<>(configureKafkaConsumer())) {
            kafkaConsumer.subscribe(Collections.singletonList(INPUT_TOPIC_NAME));

            // Premier poll pour rejoindre le groupe et obtenir l'assignation
            kafkaConsumer.poll(Duration.ofMillis(0));
            var assignment = kafkaConsumer.assignment();
            if (assignment.isEmpty()) {
                kafkaConsumer.poll(Duration.ofMillis(100));
                assignment = kafkaConsumer.assignment();
            }
            // Optionnel: relire depuis le début
            if (!assignment.isEmpty()) {
                kafkaConsumer.seekToBeginning(assignment);
            }

            Duration timeout = Duration.ofMillis(1000);
            while (true) {
                ConsumerRecords<Void, String> records = kafkaConsumer.poll(timeout);
                for (ConsumerRecord<Void, String> record : records) {
                    String raw = record.value();
                    // JSON attendu: {"#java":12,"#kafka":9,...}
                    Map<String, Integer> topMap = gson.fromJson(raw, mapType);
                    if (topMap != null) {
                        updateChart(topMap);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("something went wrong... " + e.getMessage());
        }
    }

    private void updateChart(Map<String, Integer> item) {
        // Update web server
        if (webServer != null) {
            webServer.updateLeaderboard(item);
        }
        
        // Update GUI if available
        if (guiFrame != null) {
            guiFrame.updateChart(item);
        } else {
            // Headless mode: afficher un leaderboard formaté
            System.out.println("\n" + "=".repeat(60));
            System.out.println("                   TOP HASHTAGS LEADERBOARD                  ");
            System.out.println("=".repeat(60));
            System.out.printf("%-5s %-20s %15s%n", "Rank", "Hashtag", "Count");
            System.out.println("-".repeat(60));
            
            int rank = 1;
            var leaders = new ArrayList<>(item.entrySet());
            leaders.sort(Collections.reverseOrder(Map.Entry.comparingByValue()));
            for (var entry : leaders.stream().limit(NB_LEADERS).collect(java.util.stream.Collectors.toList())) {
                System.out.printf("%-5d %-20s %,15d%n", rank++, entry.getKey(), entry.getValue());
            }
            System.out.println("=".repeat(60));
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
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "visualizor-leaderboard-" + System.currentTimeMillis());
        return consumerProperties;
    }
}
