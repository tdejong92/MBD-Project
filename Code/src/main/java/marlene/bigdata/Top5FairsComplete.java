package marlene.bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.Arrays;

import marlene.bigdata.Top5FairsComplete.ExampleMapper;
import marlene.bigdata.Top5FairsComplete.ExampleReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * MapReduce computation of the number of mentions and sentiment of the
 * available twitter data.
 * 
 * @author Tanja de Jong & Marlene Hol
 */
public class Top5FairsComplete {

	// The list of positive words used for the sentiment analysis
	private static String posWords[] = { "plezier", "genieten", "zin",
			"geweldig", "geweldige", "fantastisch", "fantastische", "goed",
			"goede", "leuk", "leuke", "lekker", "lekkere", "van de partij",
			"jippie", "gezellig", "gezellige" };
	// The list of negative words used for the sentiment analysis
	private static String negWords[] = { "dwang", "teleurstellend",
			"teleurstellende", "jammer", "saai", "saaie", "vervelend",
			"vervelende" };

	/**
	 * Class that contains the map function of the MapReduce program. The class
	 * is an extension of the class Mapper of the Apache Hadoop API
	 */
	public static class ExampleMapper extends
			Mapper<Object, Text, Text, PrintableArrayWritable> {

		//The key result of the map function
		private Text hitString = new Text();
		//The mention of the specific tweet 
		private IntWritable tweetHit = new IntWritable();
		//The sentiment score of the specific tweet
		private IntWritable tweetSentiment = new IntWritable();
		/**
		 * The ArrayWritable used to combine the number of mentions and the sentiment of the 
		 * tweet in one MapReduce function. PrintableArrayWritable is used to print the results
		 * of the ArrayWritable in a readable format.
		 */
		private PrintableArrayWritable mapArray = new PrintableArrayWritable();
		//The JSONParser used to parse the tweets
		private JSONParser parser = new JSONParser();
		//The tweet processed by the map function
		private Map tweet;
		//The list of all fairs including the hashtags and account names for that fair
		private List<Fair> fairs;

		/**
		 * The map function used to parse and evaluate the tweet. The tweet is 
		 * saved in the value parameter of the map function. 
		 * @param the key, value and context used by this map function.
		 * @throws IOException, InterruptedException
		 */
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			//Try to parse the tweet
			try {
				tweet = (Map<String, Object>) parser.parse(value.toString());
			} catch (ClassCastException e) {
				return;
			} catch (ParseException e) {
				return;
			}
			//Intialization of the list of fairs on which the tweet is checked. 
			fairs = new ArrayList<Fair>();
			fairs = Arrays.asList(
					new Fair(new Text("Huishoudbeurs"), Arrays.asList(new Text(
							"huishoudbeurs")), new Text("huishoudbeurs")),
					new Fair(new Text("Vakantiebeurs"), Arrays.asList(new Text(
							"vakantiebeurs")), new Text("devakantiebeurs")),
					new Fair(new Text("50-Plus Beurs"), Arrays.asList(new Text(
							"50plusbeurs")), new Text("50plusbeurs")),
					new Fair(new Text("Tong Tong Fair"), Arrays.asList(
							new Text("ttf11"), new Text("ttf12"), new Text(
									"ttf13"), new Text("ttf14"), new Text(
									"ttf15"), new Text("tongtongfair")),
							new Text("tongtongfair")),
					new Fair(new Text("Motorbeurs"), Arrays.asList(new Text(
							"mbu2011"), new Text("mbu2012"),
							new Text("mbu2013"), new Text("mbu2014"), new Text(
									"mbu2015"), new Text("motorbeursutrecht")),
							new Text("demotorbeurs")));
			/**
			 * Extraction of the entities of the tweet and from the entities the hashtags and user
			 * mentions used in the tweet. These hashtags and user mentions can be compared with
			 * the fair information saved in the variable fairs. 
			 */
			if (tweet != null && tweet.get("entities") != null) {
				Map<String, Object> entities = ((Map<String, Object>) tweet
						.get("entities"));
				List<Map<String, Object>> hashtagsTweet = ((List<Map<String, Object>>) entities
						.get("hashtags"));
				List<Map<String, Object>> mentionsTweet = ((List<Map<String, Object>>) entities
						.get("user_mentions"));
				String textTweet = ((String) tweet.get("text"));
				boolean hashtagFound = false;
				//Checks for all the hashtags are if the text is equal to one of the predefined hashtags
				if (!hashtagsTweet.isEmpty()) {
					for (Map<String, Object> ht : hashtagsTweet) {
						//For all the fairs is checked is checked of (one of) the hashtags equals the hashtag
						//from the tweet. The name of the fair is also used if a match is found. 
						for (int i = 0; i < fairs.size(); i++) {
							List<Text> hashtags = fairs.get(i).hashtags;
							for (int j = 0; j < hashtags.size(); j++) {
								Text hashtag = hashtags.get(j);
								//all hashtags are converted to lowercase to make sure capitalization is not
								//an issue with finding matchs. 
								if (hashtag.equals(new Text(((String) ht
										.get("text")).toLowerCase()))) {
									hashtagFound = true;
									//If a match is found it is checked if the tweet is not from the account of 
									//the fair.
									Map<String, Object> user = ((Map<String, Object>) tweet
											.get("user"));
									Text accountName = new Text(
											(String) user.get("screen_name"));
									boolean accountFound = false;
									Text account = fairs.get(i).account;
									if (accountName.equals(account)) {
										accountFound = true;
									}
									//If tweet is not from the account of the fair the tweet is relevant 
									//for our analysis. 
									if (!accountFound) {
										//Key is set to the name of the fair
										hitString.set(fairs.get(i).name);
										//The submitted count is set to 1, because this is one 
										//tweet about the consumer fair. 
										tweetHit.set(1);
										//Sentiment of the tweet is computed and the variable
										//is set to this sentiment
										tweetSentiment
												.set(getSentiment(textTweet));
										IntWritable[] ints = new IntWritable[2];
										ints[0] = tweetHit;
										ints[1] = tweetSentiment;
										//Both count and sentiment is saved in the PrintableArrayWritable
										mapArray.set(ints);
										//Key-value pair is written to the context. 
										context.write(hitString, mapArray);
									}
								}
							}
						}
					}
				}
				//If no hashtags of one of the fairs is found, the user mentions of the tweet are checked
				//to find out if the account of the fair is checked. 
				if (!hashtagFound) {
					for (Map<String, Object> mt : mentionsTweet) {
						//Also here each fair is checked seperately
						for (int i = 0; i < fairs.size(); i++) {
							Text account = fairs.get(i).account;
							//If account is tagged in the tweet this tweet is relevant
							//for our analysis. Also here the screen name is converted
							//to lowercase to make sure capitalization is not an issue. 
							if (account.equals(new Text(((String) mt
									.get("screen_name")).toLowerCase()))) {
								//Key is set to the name of the fair
								hitString.set(fairs.get(i).name);
								//The submitted count is set to 1, because this is one 
								//tweet about the consumer fair. 
								tweetHit.set(1);
								//Sentiment of the tweet is computed and the variable
								//is set to this sentiment
								tweetSentiment
										.set(getSentiment(textTweet));
								IntWritable[] ints = new IntWritable[2];
								ints[0] = tweetHit;
								ints[1] = tweetSentiment;
								//Both count and sentiment is saved in the PrintableArrayWritable
								mapArray.set(ints);
								//Key-value pair is written to the context. 
								context.write(hitString, mapArray);
							}
						}
					}
				}
			}
		}
	}
	
	
	/**
	 * Class that contains the reduce function of the MapReduce program. The class
	 * is an extension of the class Reducer of the Apache Hadoop API
	 */
	public static class ExampleReducer extends
			Reducer<Text, PrintableArrayWritable, Text, PrintableArrayWritable> {
		
		/**
		 * The reduce function to determine the number of mentions and the total sentiment 
		 * of an fair. The result is a PrintableArrayWritable with both the number of mentions
		 * and the total sentiment of the fair. 
		 * @param the key, values and context used by this reduce function.
		 * @throws IOException, InterruptedException
		 */
		public void reduce(Text key, Iterable<PrintableArrayWritable> values,
				Context context) throws IOException, InterruptedException {
			//Array that saves the results of the reduce function
			PrintableArrayWritable reduceArray = new PrintableArrayWritable();
			//Counter of the number of mentions
			int result = 0;
			//Counter of the sentiment
			int sentimentResult = 0;
			IntWritable[] temp = new IntWritable[2];
			//All results of the map function for a specific fair are seperately
			//evaluated on the mentions and sentiment score. 
			for (PrintableArrayWritable mapValue : values) {
				String[] arrayValues = mapValue.toStrings();
				//Add number of mentions of tweet to total
				result += Integer.parseInt(arrayValues[0]);
				//Add sentiment score of specific tweet to toal
				sentimentResult += Integer.parseInt(arrayValues[1]);
			}
			temp[0] = new IntWritable(result);
			temp[1] = new IntWritable(sentimentResult);
			//Results are saved in the PrintableArrayWritable
			reduceArray.set(temp);
			//Key-value pair is written to the context. Key is the same
			//as the key from the map function, the name of the fair
			context.write(key, reduceArray);
		}
	}

	/**
	 * Initialization of the MapReduce program
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: exampleTwitter <in> [<in>....] <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Twitter Reader");
		job.setJarByClass(Top5FairsComplete.class);
		job.setMapperClass(ExampleMapper.class);
		job.setReducerClass(ExampleReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PrintableArrayWritable.class);
		for (int i = 0; i < otherArgs.length - 1; i++) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	/**
	 * Function that determines the sentiment score of a specific tweet
	 * @param textTweet, the text of the tweet
	 * @return the sentiment score of the tweet
	 */
	public static int getSentiment(String textTweet) {
		//Counter of the sentiment score
		int sentiment = 0;
		
		//Method that removes the punctation from the text of the tweet and 
		//convert the text to lower case. Result is an array with only the words
		//of the tweet
		String[] words = removePunctuation(textTweet);

		//For all words in the text is checked if they equal a word in the list of 
		//positive and negative words
		for (String word : words) {
			//If a words is positive, one is added to the sentiment score
			for (String posWord : posWords) {
				sentiment = posWord.equals(word) ? sentiment + 1 : sentiment;
			}
			
			//If a word is negative, one is substracted from the sentiment score
			for (String negWord : negWords) {
				sentiment = negWord.equals(word) ? sentiment - 1 : sentiment;
			}
		}
		
		//The total sentiment score of the tweet is returned
		return sentiment;
	}

	/**
	 * Function that removes all the punctation and capitalization in a tweet text
	 * @param textTweet, the text of the tweet
	 * @return All the words of the tweet seperately
	 */
	public static String[] removePunctuation(String textTweet) {
		return textTweet.toString().replaceAll("[^a-zA-Z ]", "").toLowerCase()
				.split("\\s+");
	}
}
