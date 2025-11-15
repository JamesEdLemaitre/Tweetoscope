package tweetoscope.tweetsFilter;

import com.twitter.clientlib.model.Tweet;

/**
 * Filter tweets based on user account creation year
 * Note: Deprecated since Twitter API access plan changes
 */
public class UserAccountCreationYearTweetFilter extends TweetFilter {
    private int minYear;

    public UserAccountCreationYearTweetFilter(int minYear) {
        this.minYear = minYear;
    }

    @Override
    protected boolean match(Tweet tweet) {
        // Placeholder implementation
        // Original Twitter API v2 would check user.created_at field
        return true;
    }
}
