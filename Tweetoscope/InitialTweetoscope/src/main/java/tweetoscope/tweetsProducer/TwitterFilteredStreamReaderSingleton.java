package tweetoscope.tweetsProducer;

import java.util.List;

/**
 * Singleton for Twitter Filtered Stream Reader
 * Provides access to filtered Twitter stream using keywords
 */
public class TwitterFilteredStreamReaderSingleton extends TweetsProducer {
    private static TwitterFilteredStreamReaderSingleton instance;
    private List<String> keywords;

    private TwitterFilteredStreamReaderSingleton(List<String> keywords) {
        this.keywords = keywords;
    }

    public static TwitterFilteredStreamReaderSingleton getInstance(List<String> keywords) {
        if (instance == null) {
            instance = new TwitterFilteredStreamReaderSingleton(keywords);
        }
        return instance;
    }

    public List<String> getKeywords() {
        return keywords;
    }

    @Override
    public void run() {
        // Placeholder for Twitter stream reading
    }
}
