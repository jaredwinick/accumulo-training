package com.github.jaredwinick;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jaredwinick.model.Tweet;
import com.google.common.io.Files;

/**
 * 
 * Create an index table and record table. Index by id
 *
 */
public class Exercise4 {

	private Logger log = LoggerFactory.getLogger(Exercise4.class);

	private Mutation tweetToRecordMutation(final Pair<String, Tweet> tweetWithRecordId) {

		Mutation mutation = new Mutation(tweetWithRecordId.getFirst());

		// Serialize the whole Tweet object to a single Value
		mutation.put("tweetBytes", "", new ColumnVisibility(), new Value(tweetToBytes(tweetWithRecordId.getSecond())));

		return mutation;
	}

	private Mutation tweetToIndexMutation(final Pair<String, Tweet> tweetWithRecordId) {

		// EXERCISE EXERCISE EXERCISE
		// Create indexes just on the Tweet idStr for now. Use tweetWithRecordId.getSecond().getIdStr()
		// with the StringLexicoder
		StringLexicoder stringLexicoder = new StringLexicoder();
		Mutation mutation = null;

		// now put the field name as the CF, record id in as the CQ and an empty Value.

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
			tweet = (Tweet) objectInputStream.readObject();
		} catch (IOException e) {

		} catch (ClassNotFoundException e) {

		}

		return tweet;
	}

	public void run() throws IOException, InterruptedException,
			AccumuloException, AccumuloSecurityException, TableExistsException,
			TableNotFoundException {

		// Initialize MiniAccumuloCluster
		File tempDirectory = Files.createTempDir();
		MiniAccumuloCluster accumulo = new MiniAccumuloCluster(tempDirectory, ExerciseConstants.PASSWORD);
		accumulo.start();

		// Connect to the Accumulo instance and print out its name
		Instance instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
		Connector connector = instance.getConnector(ExerciseConstants.USER, new PasswordToken(ExerciseConstants.PASSWORD));

		// Create table for writing objects and indexes
		connector.tableOperations().create(ExerciseConstants.RECORD_TABLE);
		connector.tableOperations().create(ExerciseConstants.INDEX_TABLE);
		BatchWriter recordBatchWriter = connector.createBatchWriter(ExerciseConstants.RECORD_TABLE, new BatchWriterConfig());
		BatchWriter indexBatchWriter = connector.createBatchWriter(ExerciseConstants.INDEX_TABLE, new BatchWriterConfig());

		// Load our mock tweets. Generate Mutations for our Record table and
		// Index tables
		List<Tweet> tweets = DataLoader.loadTweets();

		// Generate record ids for each tweet. it may make sense to use "random" record ids
		// instead of properties of the recsords for the row to make sure overwrite records
		List<Pair<String, Tweet>> tweetsWithRecordIds = tweets.stream()
				.map(tweet -> new Pair<String, Tweet>(UUID.randomUUID().toString(), tweet))
				.collect(Collectors.toList());

		// Create our record table key/values
		List<Mutation> recordMutations = tweetsWithRecordIds
				.stream()
				.map(tweetWithRecordId -> tweetToRecordMutation(tweetWithRecordId))
				.collect(Collectors.toList());

		// Create our index table key/values
		List<Mutation> indexMutations = tweetsWithRecordIds
				.stream()
				.map(tweetWithRecordId -> tweetToIndexMutation(tweetWithRecordId))
				.collect(Collectors.toList());

		// Now write our record and index mutations to Accumulo
		recordBatchWriter.addMutations(recordMutations);
		recordBatchWriter.flush();
		indexBatchWriter.addMutations(indexMutations);
		indexBatchWriter.flush();

		// Print out the Key/Values just so we can see what they look like
		Util.printAllKeyValues(connector, ExerciseConstants.INDEX_TABLE);
		Util.printAllKeyValues(connector, ExerciseConstants.RECORD_TABLE);

		// EXERCISE EXERCISE EXERCISE
		// Now search for the Tweet with the given id using our index table
		// Reference https://accumulo.apache.org/1.7/accumulo_user_manual.html#_indexing
		String searchTerm = "114749583439036416";
		StringLexicoder stringLexicoder = new StringLexicoder();
		Scanner indexScanner = null;
		
		// EXERCISE EXERCISE EXERCISE
		// Build a set of record ids based on index hits
		Set<Range> matchingRows = new HashSet<Range>();
		
		// EXERCISE EXERCISE EXERCISE
		// Creat a BatchScanner to scan the record table and to
		// fetch all the matching records
		BatchScanner recordBatchScanner = null;
		
		for(Entry<Key,Value> entry : recordBatchScanner) {
		   
			// Deserialize the record table value 
			Tweet tweet = bytesToTweet(entry.getValue().get());
			log.info("Read tweet with id: {}", tweet.getIdStr());
		}	
		
		// Clean up
		accumulo.stop();
		tempDirectory.delete();
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, AccumuloException, AccumuloSecurityException,
			TableExistsException, TableNotFoundException {
		Exercise4 exercise = new Exercise4();
		exercise.run();
	}

}
