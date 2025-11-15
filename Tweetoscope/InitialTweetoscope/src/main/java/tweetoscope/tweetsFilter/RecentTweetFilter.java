package tweetoscope.tweetsFilter;

import java.time.Duration;
import java.time.OffsetDateTime;

import com.twitter.clientlib.model.Tweet;

public class RecentTweetFilter extends TweetFilter {
    protected long maxAge;

    public RecentTweetFilter(long maxAge) {
        this.maxAge = maxAge;
    }

    @Override
    protected boolean match(Tweet tweet) {
        if (tweet == null) {
            return false;
        }
        OffsetDateTime tweetDate = tweet.getCreatedAt();
        if (tweetDate == null) {
            return false;
        }
        OffsetDateTime now = OffsetDateTime.now();
        long ageHours = Duration.between(tweetDate, now).toHours();

        return ageHours <= this.maxAge;
    }
}
