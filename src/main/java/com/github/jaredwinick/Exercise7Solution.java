package com.github.jaredwinick;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jaredwinick.model.Tweet;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

/**
 * 
 * Serializes/deserializes Tweet object to/from Accumulo
 * and then deletes them. Use the solution from Exercise 3
 * but add in the deleting of Key/Values. See the Mutation class
 * for information on how to delete individual Key/Values
 *
 */
public class Exercise7Solution {
	
	private Logger log = LoggerFactory.getLogger(Exercise7Solution.class);
	
	private Mutation tweetToMutation(final Tweet tweet) {
		
		// EXERCISE EXERCISE EXERCISE
		// Row should be the Tweet.idStr
		// Serialize the entire Tweet object as the Value
		// See function tweetToBytes
		Mutation mutation = new Mutation(tweet.getIdStr());

		// Serialize the whole Tweet object to a single Value
		mutation.put("tweetBytes", "", new ColumnVisibility(), new Value(tweetToBytes(tweet)));

		return mutation;
	}
	
	private Mutation tweetToDeleteMutation(final Tweet tweet) {
		
		Mutation mutation = new Mutation(tweet.getIdStr());
		mutation.putDelete("tweetBytes", "", new ColumnVisibility());
		return mutation;
	}
	
	// Use Java serialization to turn a Tweet object into a byte array
	private byte[] tweetToBytes(final Tweet tweet) {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		ObjectOutputStream objectOutputStream;
		try {
			objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
			objectOutputStream.writeObject(tweet);
			objectOutputStream.flush();
		} catch (IOException e) {
			
		}
		
		return byteArrayOutputStream.toByteArray();
	}
	
	// User Java (de)serialization to turn a byte array back into a Tweet
	private Tweet bytesToTweet(byte[] bytes) {
		Tweet tweet = null;
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
		try {
			ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
			tweet = (Tweet)objectInputStream.readObject();
		} catch (IOException e) {

		} catch (ClassNotFoundException e) {
		
		}
		
		return tweet;
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
		
		// EXERCISE EXERCISE EXERCISE
		// Now read the tweets back from Accumulo and deserialize the Key/Values back to Tweet objects
		// Print out all the Tweet idStrs here
		Scanner scanner = connector.createScanner(ExerciseConstants.RECORD_TABLE, new Authorizations());
        scanner.setRange(new Range());
        List<Mutation> deleteMutations = Lists.newArrayList();
        for (Entry<Key,Value> entry : scanner) {
                
            Tweet tweet = bytesToTweet(entry.getValue().get());
            log.info("Read tweet with id: {}", tweet.getIdStr());
            
            deleteMutations.add(tweetToDeleteMutation(tweet));
        }
		
		// EXERCISE EXERCISE EXERCISE
		// Now delete all the Key/Values individually. See Mutation class javadoc for details.
        batchWriter.addMutations(deleteMutations);
        batchWriter.flush();
        
		// Now read the hopefully non-existent tweets back from Accumulo and just print out (nothing) for verification
		log.info("After the deletes");
        Util.printAllKeyValues(connector, ExerciseConstants.RECORD_TABLE);
		
		// Clean up
		accumulo.stop();
		tempDirectory.delete();
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
		Exercise7Solution exercise = new Exercise7Solution();
		exercise.run();
	}

}
