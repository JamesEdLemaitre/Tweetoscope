package tweetoscope.kafka;

import java.awt.Color;
import java.awt.Dimension;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Stream;

import javax.swing.BorderFactory;
import javax.swing.JFrame;
import javax.swing.SwingUtilities;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.AxisLocation;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class KafkaLeaderboard extends JFrame {

    private static final String INPUT_TOPIC_NAME = "leaderboard";
    private static final int NB_LEADERS = 5;

    // Dataset et constantes d'affichage
    private final DefaultCategoryDataset dataset = new DefaultCategoryDataset();
    private static final String ROW_KEY = "hashtag";

    // Gson + type pour Map<String,Integer> (pleinement qualifié pour éviter les conflits d'import)
    private final Gson gson = new GsonBuilder().create();
    private final java.lang.reflect.Type mapType =
            new com.google.gson.reflect.TypeToken<Map<String, Integer>>() {}.getType();

    public static void main(String[] args) {
        // Lance l'UI sur l'EDT
        SwingUtilities.invokeLater(() -> {
            KafkaLeaderboard app = new KafkaLeaderboard(NB_LEADERS);
            app.setVisible(true);
            // Démarre le consumer Kafka en arrière-plan
            app.startConsumerThread();
        });
    }

    public KafkaLeaderboard(int nbLeaders) {
        // Construction de la fenêtre et du graphe (inspiré de Visualizor)
        JFreeChart chart = ChartFactory.createBarChart(
                "Most Popular Hashtags",
                "",                        // label axe catégories
                "number of occurences",    // label axe valeurs
                dataset,
                PlotOrientation.HORIZONTAL,
                false,                     // legend
                true,                      // tooltips
                false                      // urls
        );
        chart.getCategoryPlot().setRangeAxisLocation(AxisLocation.BOTTOM_OR_RIGHT);
        chart.getCategoryPlot().setDomainAxisLocation(AxisLocation.BOTTOM_OR_RIGHT);

        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));
        chartPanel.setBackground(Color.white);
        chartPanel.setPreferredSize(new Dimension(500, 300));

        this.add(chartPanel);
        this.pack();
        this.setTitle("Tweetoscope");
        this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
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
        // Mise à jour sur l'EDT (Swing non thread-safe)
        SwingUtilities.invokeLater(() -> {
            dataset.clear();

            // Trie décroissant par occurrences et limite au NB_LEADERS
            Stream<Entry<String, Integer>> sortedTopHashtags = item.entrySet().stream()
                    .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                    .limit(NB_LEADERS);

            sortedTopHashtags.forEach(t -> dataset.setValue(t.getValue(), ROW_KEY, t.getKey()));

            // Padding si moins d'éléments que NB_LEADERS
            int size = Math.min(item.size(), NB_LEADERS);
            for (int i = size; i < NB_LEADERS; i++) {
                dataset.setValue(0, ROW_KEY, "");
            }
        });
    }

    private Properties configureKafkaConsumer() {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.VoidDeserializer");
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // from beginning
        // Pour être sûr de relire depuis le début à chaque run, on peut donner un group.id unique
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "visualizor-leaderboard-" + System.currentTimeMillis());
        return consumerProperties;
    }
}
