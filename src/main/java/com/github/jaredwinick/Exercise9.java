package com.github.jaredwinick;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.io.Files;

/**
 * Use a RegexFilter to filter Key within a specific range scan.
 * Note: As discussed, Filters should not be employed in conjunction
 * with full table scans as a matter of practice - use indexes instead.
 */
public class Exercise9 {
	
	private Logger log = LoggerFactory.getLogger(Exercise9.class);

	private Mutation wordToMutation(final String word) {
		
		Mutation wordMutation = new Mutation(word);
		wordMutation.put("count", "", "1");
		return wordMutation;
	}
	
	public void run() throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
		
		// Initialize MiniAccumuloCluster
		File tempDirectory = Files.createTempDir();
		MiniAccumuloCluster accumulo = new MiniAccumuloCluster(tempDirectory, ExerciseConstants.PASSWORD);
		accumulo.start();
		
		// Connect to the Accumulo instance and print out its name
		Instance instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
		Connector connector = instance.getConnector(ExerciseConstants.USER, new PasswordToken(ExerciseConstants.PASSWORD));

		// Create table for counts. Configure the SummingCombiner. This might 
		// typically be done in the Accumulo shell 
		// https://accumulo.apache.org/1.7/accumulo_user_manual.html#_combiners
		connector.tableOperations().create(ExerciseConstants.COUNT_TABLE);
		IteratorSetting summingCombinerSetting = 
				new IteratorSetting(15, "sum", SummingCombiner.class);
		SummingCombiner.setCombineAllColumns(summingCombinerSetting, true);
		// Use the String Encoder here for readability 
		SummingCombiner.setEncodingType(summingCombinerSetting, LongCombiner.Type.STRING);	
		connector.tableOperations().attachIterator(ExerciseConstants.COUNT_TABLE, 
				summingCombinerSetting);
		
		BatchWriter batchWriter = connector.createBatchWriter(ExerciseConstants.COUNT_TABLE, new BatchWriterConfig());
		
		// Take our document and split into words/tokens.
		String document = "Fourscore and seven years ago our fathers brought forth on this continent, a new nation, conceived in Liberty, and dedicated to the proposition that all men are created equal. Now we are engaged in a great civil war, testing whether that nation, or any nation so conceived and so dedicated, can long endure. We are met on a great battle-field of that war. We have come to dedicate a portion of that field, as a final resting place for those who here gave their lives that that nation might live. It is altogether fitting and proper that we should do this. But, in a larger sense, we can not dedicate-we can not consecrate-we can not hallow-this ground. The brave men, living and dead, who struggled here, have consecrated it, far above our poor power to add or detract. The world will little note, nor long remember what we say here, but it can never forget what they did here. It is for us the living, rather, to be dedicated here to the unfinished work which they who fought here have thus far so nobly advanced. It is rather for us to be here dedicated to the great task remaining before us-that from these honored dead we take increased devotion to that cause for which they gave the last full measure of devotion-that we here highly resolve that these dead shall not have died in vain-that this nation, under God, shall have a new birth of freedom-and that government of the people, by the people, for the people shall not perish from the earth.";
		List<Mutation> mutations = 
			Arrays.stream(document.split("\\W"))
			.filter(word -> !Strings.isNullOrEmpty(word))
			.map(word -> word.toLowerCase())
			.map(word -> wordToMutation(word))
			.collect(Collectors.toList());
		
		batchWriter.addMutations(mutations);
		batchWriter.flush();
		
		// EXERCISE EXERCISE EXERCISE
		// Find the count entries for words that start with "t" and end with "e"
		String regex = "TODO";
		Scanner scanner = connector.createScanner(ExerciseConstants.COUNT_TABLE, new Authorizations());
		IteratorSetting regexFilterSettings = new IteratorSetting(25, "regex", RegExFilter.class);
		RegExFilter.setRegexs(regexFilterSettings, regex, null, null, null, true);
		
		// Now add the iterator to the scanner and set a Range
		// and print out the results as "<word>,<count>", not just key/values
		
		
		// Clean up
		accumulo.stop();
		tempDirectory.delete();
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
		Exercise9 exercise = new Exercise9();
		exercise.run();
	}

}
