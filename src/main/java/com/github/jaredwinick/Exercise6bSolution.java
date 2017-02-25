package com.github.jaredwinick;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.github.jaredwinick.model.Tweet;
import com.google.common.io.Files;

/**
 * 
 * Uses the AccumuloOutputFormat to write from Spark
 * Uses the AccumuloInputFormat to read using Spark
 *
 */
public class Exercise6bSolution implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -5826269722224341154L;
	private Logger log = LoggerFactory.getLogger(Exercise6bSolution.class);
	
	private Mutation tweetToMutation(final Tweet tweet) {
		
		// Row will be the tweet id
        Mutation mutation = new Mutation(tweet.getIdStr());
        
        // Serialize the whole Tweet object to a single Value
        mutation.put("tweetBytes", "", new ColumnVisibility(), new Value(tweetToBytes(tweet)));

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

		final SparkConf conf = new SparkConf(true);
		final SparkContext context;

		conf.setAppName("Accumulo Training Example");
		conf.setMaster("local");

		context = new SparkContext(conf);
		JavaSparkContext javaSparkContext = new JavaSparkContext(context);
		
		// Initialize MiniAccumuloCluster	
		File tempDirectory = Files.createTempDir();
		MiniAccumuloCluster accumulo = new MiniAccumuloCluster(tempDirectory, ExerciseConstants.PASSWORD);
		accumulo.start();
		
		// Connect to the Accumulo instance and print out its name
		Instance instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
		Connector connector = instance.getConnector(ExerciseConstants.USER, new PasswordToken(ExerciseConstants.PASSWORD));

		// Create table for writing objects
		connector.tableOperations().create(ExerciseConstants.RECORD_TABLE);
		
		// Load our mock tweets which will then get written to Accumulo
		List<Tweet> tweets = DataLoader.loadTweets();
		
		JavaRDD<Tweet> tweetRdd = javaSparkContext.parallelize(tweets);
		log.info("read {} tweets in spark", tweetRdd.count());
		
		// Convert the Tweets to pairs of Text and Mutation objects. These are the required
		// types for the AccumuloOutputFormat
		// The Text is the name of the table to write the mutations to. I think it can be 
		// set to null of you call AccumuloOutputFormat.setDefaultTableName
		JavaPairRDD<Text,Mutation> keyValues = 
				tweetRdd.map(tweet -> tweetToMutation(tweet))
						.mapToPair(mutation -> new Tuple2<Text, Mutation>(new Text(ExerciseConstants.RECORD_TABLE), mutation));
		
		Job job = Job.getInstance(context.hadoopConfiguration());
		job.setOutputFormatClass(AccumuloOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);
		AccumuloOutputFormat.setConnectorInfo(job, ExerciseConstants.USER,  new PasswordToken(ExerciseConstants.PASSWORD));
		AccumuloOutputFormat.setZooKeeperInstance(job, accumulo.getClientConfig());
		AccumuloOutputFormat.setDefaultTableName(job, ExerciseConstants.RECORD_TABLE);
		AccumuloOutputFormat.setCreateTables(job, true);
		
		// Saves the data in Spark to Accumulo via the AccumuloOutputFormat
		keyValues.saveAsNewAPIHadoopDataset(job.getConfiguration());
	
		Util.printAllKeyValues(connector, ExerciseConstants.RECORD_TABLE);
		
		// Clean up
		context.stop();
		accumulo.stop();
		tempDirectory.delete();
		
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
		Exercise6bSolution exercise = new Exercise6bSolution();
		exercise.run();
	}

}
