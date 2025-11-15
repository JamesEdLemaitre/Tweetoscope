package tweetoscope.tweetsProducer;

/**
 * Singleton for Twitter Sampled Stream Reader
 * Provides access to the Twitter 1% sampled stream
 */
public class TwitterSampledStreamReaderSingleton extends TweetsProducer {
    private static TwitterSampledStreamReaderSingleton instance;

    private TwitterSampledStreamReaderSingleton() {
    }

    public static TwitterSampledStreamReaderSingleton getInstance() {
        if (instance == null) {
            instance = new TwitterSampledStreamReaderSingleton();
        }
        return instance;
    }

    @Override
    public void run() {
        // Placeholder for Twitter stream reading
    }
}
