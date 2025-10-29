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
package tweetoscope;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import tweetoscope.tweetsFilter.CountryCodeTweetFilter;
import tweetoscope.tweetsFilter.EmptyTweetFilter;
import tweetoscope.tweetsFilter.LangTweetFilter;
import tweetoscope.tweetsFilter.TweetFilter;
import tweetoscope.tweetsFilter.UserAccountCreationYearTweetFilter;
import tweetoscope.tweetsProducer.MockTwitterStreamRandom;
import tweetoscope.tweetsProducer.MockTwitterStreamRecorded;
import tweetoscope.tweetsProducer.MockTwitterStreamScenario;
import tweetoscope.tweetsProducer.TweetsProducer;
import tweetoscope.tweetsProducer.TwitterFilteredStreamReaderSingleton;
import tweetoscope.tweetsProducer.TwitterSampledStreamReaderSingleton;

/**
 * Observes Twitter hashtags trends.
 * 
 * @author Virginie Galtier
 *
 */
public class TweetoscopeApp {
	/* Possible Online Sources  */
	/* !!! deprecated from April 2023 on, the "Free" access plan no longer allow to read Tweets. */
	private static final String SAMPLED_SOURCE = "sampled";     // 1% of all Tweets
	private static final String FILTERED_SOURCE = "filtered";   // Tweets featuring selected keywords
	/* Possible Offline Sources */
	private static final String RECORDED_SOURCE = "recorded";   // Real Tweets recorded into a file
	private static final String RANDOM_SOURCE = "random";       // Randomly crafted Tweets
	protected static final String SCENARIO_SOURCE = "scenario"; // Specifically crafted Tweets
	/* Possible Filtering Criteria */
	private static final String FILTER_NONE = "none";           // keeps all Tweets
	private static final String FILTER_LANGUAGE = "language";   // keeps only Tweets written in a given language
	private static final String FILTER_ACCOUNT_CREATION_YEAR = "accountCreationYear"; // deprecated since the change in access plans
	private static final String FILTER_COUNTRY = "country"; // deprecated since the change in access plans
	
	
	// DEFAULT CONFIGURATION:
	/**
	 * source of the Tweets injected into the application
	 */
	protected String tweetsSource = RECORDED_SOURCE;
	protected String fileName = "../TestBases/miniTestBase.txt"; // For RECORDED_SOURCE only 
	/**
	 * Tweets filtering strategy
	 */
	protected String filteringStrategy = FILTER_NONE;
	/**
	 * Number of hashtags to display on the leader board
	 */
	protected int nbLeaders = 5;

	public static void main(String[] args) {
		new TweetoscopeApp(args);
	}

	public TweetoscopeApp(String[] args) {
		// reads configuration from the command line
		readProgArgs(args);

		// creates the components
		// -----------------------
		// source of the Tweets
		TweetsProducer tweetsProducer = null;
		switch (tweetsSource) {
		case SCENARIO_SOURCE:
			tweetsProducer = new MockTwitterStreamScenario();
			break;
		case RANDOM_SOURCE:
			tweetsProducer = new MockTwitterStreamRandom();
			break;
		case RECORDED_SOURCE:
			tweetsProducer = new MockTwitterStreamRecorded(fileName);
			break;
		case FILTERED_SOURCE:
			// up to 5 rules, up to 243 characters per rule
			// Some words are not usable in query ("you" for instance) and trigger an error
			// message
			// "Rules must contain at least one positive, non-stopword clause"
			// but I couldn't find a list of the stopwords online.
			List<String> keywords = new ArrayList<String>();
			keywords.add("Europe OR Africa OR Asia OR America OR Autralia OR Antartica "
					+ "OR Paris OR Berlin OR London OR Moscow OR Washington OR Beijing "
					+ "OR France OR Germany OR United Kingdom OR Russia OR USA OR China OR Brazil OR Israel "
					+ "OR Atlantic OR Pacific" + "OR place");

			keywords.add("climate OR sustainable OR energy OR free OR open OR peace OR war OR sport "
					+ "OR health OR Internet OR technology OR music OR award OR movie OR star OR pride "
					+ "OR food OR drink OR meal OR recipe OR routine OR economy OR business OR market OR science");

			keywords.add("Machine Learning OR data OR cloud OR social OR network OR woke OR election "
					+ "OR topic OR olympic OR game OR running OR trail OR workout OR challenge OR brand "
					+ "OR marketing OR advertisement OR news OR blockchain OR online OR store OR fashion");

			keywords.add("Monday OR Tuesday OR Wednesday OR Thursday OR Friday OR Saturday OR Sunday " + "OR 2022 "
					+ "OR time " + "OR week OR month OR year " + "OR yesterday OR today OR tomorrow "
					+ "OR last OR next OR soon " + "OR what OR who OR whom OR where OR when OR why");

			keywords.add("Apple OR Google OR Microsoft OR Amazon OR Facebook OR Coca-Cola OR Disney OR Samsung "
					+ "OR Intel OR NIKE OR Cisco 0R Oracle " + "OR Visa OR IBM OR Ikea " + "OR Netflix OR BMW "
					+ "OR Dion OR Thunberg " + "OR Messi OR Federer OR Djokovic " + "OR Rihanna OR Coldplay");

			tweetsProducer = TwitterFilteredStreamReaderSingleton.getInstance(keywords);
			break;
		case SAMPLED_SOURCE:
			tweetsProducer = TwitterSampledStreamReaderSingleton.getInstance();
			break;
		}

		// filters out Tweets based on the location, or language for instance
		TweetFilter filter = null;
		switch (filteringStrategy) {
		case FILTER_NONE:
			filter = new EmptyTweetFilter();
			break;
		case FILTER_LANGUAGE:
			filter = new LangTweetFilter("en");
			break;
		case FILTER_COUNTRY:
			filter = new CountryCodeTweetFilter("us");
			break;
		case FILTER_ACCOUNT_CREATION_YEAR:
			filter = new UserAccountCreationYearTweetFilter(2012);
			break;
		}

		// extracts hashtags from Tweet text
		HashtagExtractor extractor = new HashtagExtractor();

		// records the number of occurrences of each hashtag
		HashtagCounter counter = new HashtagCounter(nbLeaders);

		// visualizes the most popular hashtags as an updating histogram
		Visualizor visualizor = new Visualizor(nbLeaders);

		// defines the flow of information between the components
		// -------------------------------------------------------
		tweetsProducer.subscribe(filter);
		filter.subscribe(extractor);
		extractor.subscribe(counter);
		counter.subscribe(visualizor);

		// starts the Tweets sources
		// --------------------------
		Thread tweetsProducerThread = new Thread(tweetsProducer);
		tweetsProducerThread.start();
	}

	/**
	 * reads the values for the Tweets source, filtering strategy and number of
	 * Tweets on the leader board from the command line, and performs a few checks
	 * 
	 * @param args command line arguments
	 */
	protected void readProgArgs(String[] args) {
		List<String> validTweetsSources = Arrays.asList(SCENARIO_SOURCE, RANDOM_SOURCE, RECORDED_SOURCE,
				FILTERED_SOURCE, SAMPLED_SOURCE);
		List<String> validFilteringStrategies = Arrays.asList(FILTER_NONE, FILTER_LANGUAGE, FILTER_COUNTRY,
				FILTER_ACCOUNT_CREATION_YEAR);

		Options options = new Options();

		Option tweetsSourceOption = new Option("s", "source", true,
				"Tweets source in " + validTweetsSources.toString());
		tweetsSourceOption.setRequired(false);
		tweetsSourceOption.setArgs(1);
		tweetsSourceOption.setType(String.class);
		options.addOption(tweetsSourceOption);

		Option filteringStrategyOption = new Option("f", "filtering", true,
				"Filtering strategy in " + validFilteringStrategies.toString());
		filteringStrategyOption.setRequired(false);
		filteringStrategyOption.setArgs(1);
		filteringStrategyOption.setType(String.class);
		options.addOption(filteringStrategyOption);

		Option nbTopOptions = new Option("n", "nbTop", true, "Number of Tweets on the top-board");
		nbTopOptions.setRequired(false);
		nbTopOptions.setArgs(1);
		nbTopOptions.setType(Integer.class);
		options.addOption(nbTopOptions);

		CommandLineParser parser = new DefaultParser();
		HelpFormatter help = new HelpFormatter();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			System.out.println(e.getMessage());
			help.printHelp("Tweetoscope app command line", options);
			System.exit(1);
		}

		if (cmd.hasOption(tweetsSourceOption)) {
			tweetsSource = cmd.getOptionValue(tweetsSourceOption);
			if (!validTweetsSources.contains(tweetsSource)) {
				System.out.println("invalid Tweets source");
				help.printHelp("Tweetoscope app command line", options);
				System.exit(1);
			}
		}

		if (cmd.hasOption(filteringStrategy)) {
			filteringStrategy = cmd.getOptionValue(filteringStrategyOption);
			if (!validFilteringStrategies.contains(filteringStrategy)) {
				System.out.println("invalid filtering strategy");
				help.printHelp("Tweetoscope app command line", options);
				System.exit(1);
			}
		}

		if (cmd.hasOption(nbTopOptions)) {
			try {
				nbLeaders = Integer.parseInt(cmd.getOptionValue(nbTopOptions));
			} catch (java.lang.NumberFormatException e) {
				System.out.println("invalid number of Tweets on the top-board");
				help.printHelp("Tweetoscope app command line", options);
				System.exit(1);
			}
		}
	}
}