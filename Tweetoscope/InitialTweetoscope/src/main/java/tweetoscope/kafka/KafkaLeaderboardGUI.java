package tweetoscope.kafka;

import java.awt.Color;
import java.awt.Dimension;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import javax.swing.BorderFactory;
import javax.swing.JFrame;
import javax.swing.SwingUtilities;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.AxisLocation;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;

/**
 * GUI Frame for displaying leaderboard data - only used in non-headless mode
 */
public class KafkaLeaderboardGUI extends JFrame {

    private static final int NB_LEADERS = 5;
    private static final String ROW_KEY = "hashtag";

    private final DefaultCategoryDataset dataset = new DefaultCategoryDataset();

    public KafkaLeaderboardGUI(String title) {
        // Construction de la fenêtre et du graphe
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
        this.setTitle(title);
        this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }

    public void updateChart(Map<String, Integer> item) {
        // Mise à jour sur l'EDT (Swing non thread-safe)
        SwingUtilities.invokeLater(() -> {
            dataset.clear();

            // Trie décroissant par occurrences et limite au NB_LEADERS
            Stream<Map.Entry<String, Integer>> sortedTopHashtags = item.entrySet().stream()
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
}
