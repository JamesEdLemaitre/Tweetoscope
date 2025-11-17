package tweetoscope.kafka;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonSerializer;
import com.twitter.clientlib.model.Tweet;

import tweetoscope.tweetsFilter.EmptyTweetFilter;
import tweetoscope.tweetsFilter.LangTweetFilter;
import tweetoscope.tweetsFilter.RecentTweetFilter;
import tweetoscope.tweetsFilter.TweetFilter;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Unit tests for KafkaTweetFilter class
 * Tests the filtering functionality of tweets
 */
public class KafkaTweetFilterTest {

    private Gson gson;
    private TweetFilter emptyFilter;
    private TweetFilter langFilter;
    private TweetFilter recentFilter;

    @BeforeEach
    public void setUp() {
        // Initialize Gson with custom serializers
        gson = new GsonBuilder()
                .registerTypeAdapter(OffsetDateTime.class,
                        (JsonSerializer<OffsetDateTime>) (src, typeOfSrc, context) ->
                                context.serialize(src.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)))
                .registerTypeAdapter(OffsetDateTime.class,
                        (JsonDeserializer<OffsetDateTime>) (json, typeOfT, context) ->
                                OffsetDateTime.parse(json.getAsString(), DateTimeFormatter.ISO_OFFSET_DATE_TIME))
                .create();

        // Initialize filters
        emptyFilter = new EmptyTweetFilter();
        langFilter = new LangTweetFilter("en");
        recentFilter = new RecentTweetFilter(24);
    }

    /**
     * Test that EmptyTweetFilter accepts any non-null tweet
     */
    @Test
    public void testEmptyTweetFilterAcceptsAllTweets() {
        // Create a sample tweet JSON
        String tweetJson = "{\"id\":\"123456789\",\"text\":\"Hello world\"}";
        Tweet tweet = gson.fromJson(tweetJson, Tweet.class);

        assertNotNull(tweet, "Tweet should not be null");
        assertTrue(emptyFilter.isMatching(tweet), "EmptyTweetFilter should accept all tweets");
    }

    /**
     * Test that LangTweetFilter accepts tweets in the specified language
     */
    @Test
    public void testLangTweetFilterAcceptsEnglish() {
        // Create a sample English tweet
        String tweetJson = "{\"id\":\"123456789\",\"text\":\"Hello world\",\"lang\":\"en\"}";
        Tweet tweet = gson.fromJson(tweetJson, Tweet.class);

        assertNotNull(tweet, "Tweet should not be null");
        assertTrue(langFilter.isMatching(tweet), "LangTweetFilter should accept English tweets");
    }

    /**
     * Test that LangTweetFilter rejects tweets not in the specified language
     */
    @Test
    public void testLangTweetFilterRejectsFrench() {
        // Create a sample French tweet
        String tweetJson = "{\"id\":\"987654321\",\"text\":\"Bonjour le monde\",\"lang\":\"fr\"}";
        Tweet tweet = gson.fromJson(tweetJson, Tweet.class);

        assertNotNull(tweet, "Tweet should not be null");
        assertFalse(langFilter.isMatching(tweet), "LangTweetFilter should reject non-English tweets");
    }

    /**
     * Test that RecentTweetFilter accepts recent tweets
     */
    @Test
    public void testRecentTweetFilterAcceptsRecentTweets() {
        // Create a tweet with a recent timestamp (now)
        OffsetDateTime now = OffsetDateTime.now();
        String recentTweetJson = "{\"id\":\"111111111\",\"text\":\"Recent tweet\",\"created_at\":\"" + 
                now.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) + "\"}";
        Tweet recentTweet = gson.fromJson(recentTweetJson, Tweet.class);

        assertNotNull(recentTweet, "Recent tweet should not be null");
        assertTrue(recentFilter.isMatching(recentTweet), "RecentTweetFilter should accept tweets from the last 24 hours");
    }

    /**
     * Test that RecentTweetFilter rejects old tweets
     */
    @Test
    public void testRecentTweetFilterRejectsOldTweets() {
        // Create a tweet with an old timestamp (30 days ago)
        OffsetDateTime oldDate = OffsetDateTime.now().minusDays(30);
        String oldTweetJson = "{\"id\":\"222222222\",\"text\":\"Old tweet\",\"created_at\":\"" + 
                oldDate.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) + "\"}";
        Tweet oldTweet = gson.fromJson(oldTweetJson, Tweet.class);

        assertNotNull(oldTweet, "Old tweet should not be null");
        assertFalse(recentFilter.isMatching(oldTweet), "RecentTweetFilter should reject tweets older than 24 hours");
    }

    /**
     * Test that Gson can properly serialize and deserialize tweets
     */
    @Test
    public void testGsonSerializationDeserialization() {
        String tweetJson = "{\"id\":\"333333333\",\"text\":\"Serialization test\",\"lang\":\"en\"}";
        Tweet tweet = gson.fromJson(tweetJson, Tweet.class);

        assertNotNull(tweet, "Tweet should not be null after deserialization");
        assertEquals("333333333", tweet.getId(), "Tweet ID should match");
        assertEquals("Serialization test", tweet.getText(), "Tweet text should match");

        // Serialize back to JSON
        String serializedJson = gson.toJson(tweet);
        assertNotNull(serializedJson, "Serialized JSON should not be null");
        assertTrue(serializedJson.contains("333333333"), "Serialized JSON should contain tweet ID");
    }

    /**
     * Test that null or empty tweets are handled properly
     */
    @Test
    public void testNullTweetHandling() {
        String emptyJson = "";
        Tweet result = gson.fromJson(emptyJson, Tweet.class);
        assert result == null : "Empty JSON should deserialize to null";
    }
}
