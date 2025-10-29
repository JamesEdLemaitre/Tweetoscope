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
package tweetoscope.tweetsProducer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.util.concurrent.Flow.Subscriber;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.twitter.clientlib.model.Tweet;

/**
 * Mimics the TwitterStreamSampleReaderSingleton class. To be used when the
 * Twitter sampled stream rate limit is exceeded for instance. Replays Tweets
 * recorded previously from the Twitter API.
 * 
 * @author Virginie Galtier
 *
 */
public final class MockTwitterStreamRecorded extends OfflineTweetsProducer {

	/**
	 * File that holds the recorded Tweets
	 */
	protected String fileName;

	/**
	 * Creates a MockTwitterStreamRecorded.
	 */
	public MockTwitterStreamRecorded(String fileName) {
		super();

		this.fileName = fileName;
	}

	/**
	 * Reads Tweets from a file and forwards them to the Java Flow subscribers.
	 */
	@Override
	public void run() {
		try {
			String jsonString = new String(Files.readAllBytes(Paths.get(fileName)));
			Gson gson = new GsonBuilder()
					.registerTypeAdapter(OffsetDateTime.class, (JsonDeserializer<OffsetDateTime>) (json, type,
							context) -> OffsetDateTime.parse(json.getAsString()))
					.create();
			JsonObject jsonObject = gson.fromJson(jsonString, JsonObject.class);
			JsonArray prerecordedtweets = jsonObject.getAsJsonArray("tweets");
			for (JsonElement je : prerecordedtweets) {
				Tweet tweet = gson.fromJson(je, Tweet.class);
				for (Subscriber<? super Tweet> s : subscribers) {
					s.onNext(tweet);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}