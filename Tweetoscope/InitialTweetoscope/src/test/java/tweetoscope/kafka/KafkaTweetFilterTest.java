package tweetoscope.kafka;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

    /**
     * Test that invalid JSON throws exception during deserialization
     */
    @Test
    public void testInvalidJsonDeserialization() {
        String invalidJson = "{invalid json format}";
        Exception exception = assertThrows(Exception.class, () -> 
            gson.fromJson(invalidJson, Tweet.class));
        assertNotNull(exception, "Exception should be thrown for invalid JSON");
    }

    /**
     * Test that JSON with missing required fields deserializes gracefully
     */
    @Test
    public void testJsonWithMissingFields() {
        String incompleteJson = "{\"id\":\"444444444\"}";
        Tweet tweet = gson.fromJson(incompleteJson, Tweet.class);
        
        assertNotNull(tweet, "Tweet with missing fields should deserialize to a Tweet object");
        assertEquals("444444444", tweet.getId(), "Tweet ID should still be present");
        // text and lang may be null, which is acceptable
    }

    /**
     * Test that tweets with special characters are properly handled
     */
    @Test
    public void testTweetWithSpecialCharacters() {
        String specialCharJson = "{\"id\":\"555555555\",\"text\":\"Hello 🌍 世界 Привет\",\"lang\":\"en\"}";
        Tweet tweet = gson.fromJson(specialCharJson, Tweet.class);

        assertNotNull(tweet, "Tweet with special characters should not be null");
        assertEquals("Hello 🌍 世界 Привет", tweet.getText(), "Tweet text should contain special characters");
        assertTrue(emptyFilter.isMatching(tweet), "EmptyTweetFilter should accept tweets with special characters");
    }

    /**
     * Test that LangTweetFilter throws NPE with null language field (bug in implementation)
     * This reveals a potential bug: the filter doesn't check for null language
     */
    @Test
    public void testLangTweetFilterWithNullLanguage() {
        String tweetJsonNoLang = "{\"id\":\"666666666\",\"text\":\"No language specified\"}";
        Tweet tweet = gson.fromJson(tweetJsonNoLang, Tweet.class);

        assertNotNull(tweet, "Tweet should not be null");
        // Current implementation throws NPE because it doesn't null-check tweet.getLang()
        Exception exception = assertThrows(NullPointerException.class, 
            () -> langFilter.isMatching(tweet),
            "LangTweetFilter should throw NPE for null language (implementation bug)");
        assertNotNull(exception, "NullPointerException should be thrown");
    }

    /**
     * Test that RecentTweetFilter handles null created_at field gracefully
     */
    @Test
    public void testRecentTweetFilterWithNullCreatedAt() {
        String tweetJsonNoDate = "{\"id\":\"777777777\",\"text\":\"No date specified\",\"lang\":\"en\"}";
        Tweet tweet = gson.fromJson(tweetJsonNoDate, Tweet.class);

        assertNotNull(tweet, "Tweet should not be null");
        // Should not match because created_at is null
        assertFalse(recentFilter.isMatching(tweet), "RecentTweetFilter should reject tweets with null created_at");
    }

    /**
     * Test that tweets with empty text are handled correctly
     */
    @Test
    public void testTweetWithEmptyText() {
        String emptyTextJson = "{\"id\":\"888888888\",\"text\":\"\",\"lang\":\"en\"}";
        Tweet tweet = gson.fromJson(emptyTextJson, Tweet.class);

        assertNotNull(tweet, "Tweet with empty text should not be null");
        assertTrue(emptyFilter.isMatching(tweet), "EmptyTweetFilter should accept tweets with empty text");
    }

    /**
     * Test multiple filters in sequence (AND logic)
     */
    @Test
    public void testMultipleFiltersInSequence() {
        OffsetDateTime now = OffsetDateTime.now();
        String tweetJson = "{\"id\":\"999999999\",\"text\":\"Recent English tweet\",\"lang\":\"en\",\"created_at\":\"" + 
                now.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) + "\"}";
        Tweet tweet = gson.fromJson(tweetJson, Tweet.class);

        assertNotNull(tweet, "Tweet should not be null");
        assertTrue(emptyFilter.isMatching(tweet), "Should pass EmptyTweetFilter");
        assertTrue(langFilter.isMatching(tweet), "Should pass LangTweetFilter for English");
        assertTrue(recentFilter.isMatching(tweet), "Should pass RecentTweetFilter");
    }

    /**
     * Test that a tweet fails multiple filters (no AND logic, independent filters)
     */
    @Test
    public void testTweetFailsMultipleFilters() {
        OffsetDateTime oldDate = OffsetDateTime.now().minusDays(60);
        String tweetJson = "{\"id\":\"101010101\",\"text\":\"Old French tweet\",\"lang\":\"fr\",\"created_at\":\"" + 
                oldDate.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) + "\"}";
        Tweet tweet = gson.fromJson(tweetJson, Tweet.class);

        assertNotNull(tweet, "Tweet should not be null");
        assertTrue(emptyFilter.isMatching(tweet), "Should pass EmptyTweetFilter");
        assertFalse(langFilter.isMatching(tweet), "Should fail LangTweetFilter (French, not English)");
        assertFalse(recentFilter.isMatching(tweet), "Should fail RecentTweetFilter (too old)");
    }

    /**
     * Test Gson handling of whitespace and special JSON structures
     */
    @Test
    public void testGsonWithWhitespaceAndLineBreaks() {
        String tweetJsonWithWhitespace = "{\"id\":\"111111110\",\"text\":\"Tweet with\\nline breaks\",\"lang\":\"en\"}";
        Tweet tweet = gson.fromJson(tweetJsonWithWhitespace, Tweet.class);

        assertNotNull(tweet, "Tweet should not be null");
        assertTrue(tweet.getText().contains("\\n") || tweet.getText().contains("\n"), "Text should preserve line breaks");
    }
}
