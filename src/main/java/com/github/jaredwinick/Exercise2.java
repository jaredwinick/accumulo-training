package com.github.jaredwinick;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;

import com.github.jaredwinick.model.Tweet;
import com.google.common.io.Files;

/**
 * 
 * Writes Tweet objects to Accumulo and scans the Key/Values for verification
 *
 */
public class Exercise2 {
	
	private Mutation tweetToMutation(final Tweet tweet) {
		
		// Row will be the tweet id
		Mutation mutation = new Mutation(tweet.getIdStr());
		
		mutation.put("idStr", "", new ColumnVisibility(), tweet.getIdStr());
		mutation.put("userId", "", new ColumnVisibility(), tweet.getUserId().toString());
		mutation.put("userName", "", new ColumnVisibility(), tweet.getUserName());
		mutation.put("createdAt", "", new ColumnVisibility(), ((Long)tweet.getCreatedAt().getTime()).toString());		
		
		return mutation;
	}
	
	
	public void run() throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
		
		// Initialize MiniAccumuloCluster
		File tempDirectory = Files.createTempDir();
		MiniAccumuloCluster accumulo = new MiniAccumuloCluster(tempDirectory, ExerciseConstants.PASSWORD);
		accumulo.start();
		
		// Connect to the Accumulo instance and print out its name
		Instance instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
		Connector connector = instance.getConnector(ExerciseConstants.USER, new PasswordToken(ExerciseConstants.PASSWORD));

		// Create table for writing objects
		connector.tableOperations().create(ExerciseConstants.RECORD_TABLE);
		BatchWriter batchWriter = connector.createBatchWriter(ExerciseConstants.RECORD_TABLE, new BatchWriterConfig());
		
		// Load our mock tweets which will then get written to Accumulo
		// Make sure to explicitly flush the BatchWriter here as it otherwise
		// flushes happen asynchronously based on BatchWriter configuration of
		// timeouts or buffer size
		List<Tweet> tweets = DataLoader.loadTweets();
		List<Mutation> mutations = tweets.stream()
										   .map(tweet -> tweetToMutation(tweet))
										   .collect(Collectors.toList());									   
		batchWriter.addMutations(mutations);
		batchWriter.flush();
		
		
		// Now read the tweets back from Accumulo and just print out for verification
		Util.printAllKeyValues(connector, ExerciseConstants.RECORD_TABLE);
		
		// Clean up
		accumulo.stop();
		tempDirectory.delete();
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
		Exercise2 exercise = new Exercise2();
		exercise.run();
	}

}
