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
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
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
import com.google.common.collect.Lists;
import com.google.common.io.Files;

/**
 * 
 * Create an index table and record table. Index by id
 *
 */
public class Exercise4Solution {

	private Logger log = LoggerFactory.getLogger(Exercise4Solution.class);

	private Mutation tweetToRecordMutation(final Pair<String, Tweet> tweetWithRecordId) {

		Mutation mutation = new Mutation(tweetWithRecordId.getFirst());

		// Serialize the whole Tweet object to a single Value
		mutation.put("tweetBytes", "", new ColumnVisibility(), new Value(tweetToBytes(tweetWithRecordId.getSecond())));

		return mutation;
	}

	private List<Mutation> tweetToIndexMutation(final Pair<String, Tweet> tweetWithRecordId) {

		List<Mutation> mutations = Lists.newArrayList();
		
		// EXERCISE EXERCISE EXERCISE
		// Create indexes just on the Tweet idStr and userId
		// Use tweetWithRecordId.getSecond().getIdStr()
		StringLexicoder stringLexicoder = new StringLexicoder();
		LongLexicoder longLexicoder = new LongLexicoder();
		
		Mutation idStringMutation = new Mutation(stringLexicoder.encode(tweetWithRecordId.getSecond().getIdStr()));
		idStringMutation.put("idStr", tweetWithRecordId.getFirst(), new ColumnVisibility(), new Value());

		// Also add index entry for userId
		Mutation userIdMutation = new Mutation(longLexicoder.encode(tweetWithRecordId.getSecond().getUserId())); 
		userIdMutation.put("userId", tweetWithRecordId.getFirst(), new ColumnVisibility(), new Value());

		mutations.add(idStringMutation);
		mutations.add(userIdMutation);

		return mutations;
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
				.flatMap(tweetWithRecordId -> tweetToIndexMutation(tweetWithRecordId).stream())
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
        Scanner indexScanner = connector.createScanner(ExerciseConstants.INDEX_TABLE, new Authorizations());
        indexScanner.setRange(Range.exact(new Text(stringLexicoder.encode(searchTerm))));
		indexScanner.fetchColumnFamily(new Text("idStr"));
		
        // To search for different field/values in the index table, we just have to
        // build the Range for the indexScanner differently and fetch a different CF
        // For instance to find userIds between 0 and 15000
		LongLexicoder longLexicoder = new LongLexicoder();
		Long startUserId = 0L;
		Long endUserId = 15000L;
		Range userIdRange = new Range(
				new Text(longLexicoder.encode(startUserId)),
				new Text(longLexicoder.encode(endUserId)));
		//indexScanner.fetchColumnFamily(new Text("userId"));
		//indexScanner.setRange(userIdRange);
		
		// EXERCISE EXERCISE EXERCISE
		// Build a set of record ids based on index hits
		Set<Range> matchingRows = new HashSet<Range>();
		for(Entry<Key,Value> entry : indexScanner) {
            matchingRows.add(new Range(entry.getKey().getColumnQualifier()));
        }
		
		// EXERCISE EXERCISE EXERCISE
		// Creat a BatchScanner to scan the record table and to
		// fetch all the matching records
		BatchScanner recordBatchScanner = connector.createBatchScanner(
				ExerciseConstants.RECORD_TABLE, new Authorizations(), 10);
		recordBatchScanner.setRanges(matchingRows);
		for (Entry<Key, Value> entry : recordBatchScanner) {

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
		Exercise4Solution exercise = new Exercise4Solution();
		exercise.run();
	}

}
