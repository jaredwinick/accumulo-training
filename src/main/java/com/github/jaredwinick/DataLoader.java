package com.github.jaredwinick;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codehaus.jackson.map.ObjectMapper;

import com.github.jaredwinick.model.Tweet;


public class DataLoader {

	private static Stream<Tweet> deserializeTweet(ObjectMapper mapper, String tweetJson) {
		Stream<Tweet> tweetStream;
		
		try {
			tweetStream = Stream.of(mapper.readValue(tweetJson, Tweet.class));
		}
		catch (Throwable e) {
			tweetStream = Stream.empty();
		}
		
		return tweetStream;
	}
	
	public static List<Tweet> loadTweets() throws IOException {
		
		// Setup Jackson to parse the mock tweets data
		ObjectMapper mapper = new ObjectMapper();
		SimpleDateFormat df = new SimpleDateFormat("MM-dd-yyyy hh:mm:ss");
		mapper.setDateFormat(df);
		
		File jsonLinesFile = new File("src/main/resources/data/tweets.jsonl");
		BufferedReader bufferedReader = new BufferedReader(new FileReader(jsonLinesFile));
	
		List<Tweet>	tweetList = 
				bufferedReader.lines()
						      .flatMap(line -> deserializeTweet(mapper, line))
						      .collect(Collectors.toList());
	
		bufferedReader.close();
		
		return tweetList;
	
	}
}
