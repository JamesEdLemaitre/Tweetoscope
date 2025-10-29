/*
Copyright 2022 Virginie Galtier

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>
 */
package tweetoscope.tweetsFilter;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashSet;

import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.api.UsersApi.APIfindUserByIdRequest;
import com.twitter.clientlib.model.Get2UsersIdResponse;
import com.twitter.clientlib.model.Tweet;

/**
 * Filter Tweets according to the creation year of the author's account
 * 
 * @author Virginie Galtier
 *
 */
public class UserAccountCreationYearTweetFilter extends OnlineTweetFilter {

	/**
	 * The targeted max creation year (excluded)
	 */
	protected int creationYear;

	public UserAccountCreationYearTweetFilter(int creationYear) {
		this.creationYear = creationYear;
	}

	/**
	 * return true if the tweet was posted by an account created before 01/01/creationYear
	 */
	@Override
	protected boolean match(Tweet tweet) {
		OffsetDateTime creationDate = getUserAccountCreationDate(tweet);
		OffsetDateTime limitDate = OffsetDateTime.of(creationYear, 01, 01, 0, 0, 0, 0, ZoneOffset.UTC);
		if (creationDate != null && creationDate.isBefore(limitDate)) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Return the creation date of the account of the user who posted the tweet.
	 * @param tweet
	 * @return the creation date of the account of the user who posted the tweet if the information is available, null otherwise
	 */
	private OffsetDateTime getUserAccountCreationDate(Tweet tweet) {
		if (tweet.getAuthorId()!= null) {
			try {
				APIfindUserByIdRequest request = twitterApiInstance.users().findUserById(tweet.getAuthorId());
				Get2UsersIdResponse response = request.execute();
				if (response.getData() != null) {
					return response.getData().getCreatedAt();
				}
			} catch (ApiException e) {
				System.err.println("error while retreiving the user account creation date: " + e.getResponseBody());
			}
		}
		return null;
	}
}
