package tweetoscope.tweetsFilter;

import com.twitter.clientlib.model.Tweet;

import java.time.Duration;
import java.time.OffsetDateTime;

public class RecentTweetFilter extends TweetFilter {
    protected long maxAge;

    public RecentTweetFilter(long maxAge) {
        this.maxAge = maxAge;
    }

    @Override
    protected boolean match(Tweet tweet) {
        OffsetDateTime tweetDate = tweet.getCreatedAt();
        OffsetDateTime now = OffsetDateTime.now();
        long ageHours = Duration.between(tweetDate, now).toHours();

        return ageHours <= this.maxAge;
    }
}
