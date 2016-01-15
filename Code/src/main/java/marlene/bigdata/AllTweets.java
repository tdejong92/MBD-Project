package marlene.bigdata;

import java.io.IOException;
import java.util.Map;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Class that counts the total number of tweets in a certain data set by using MapReduce
 * @author Tanja de Jong & Marlene Hol
 */
public class AllTweets {

	/**
	 * Class that contains the map function of the MapReduce program tha counts the total number
	 * of tweets. The class is an extension of the Mapper from the Apache Hadoop Framework. 
	 */
	public static class ExampleMapper extends Mapper<Object,Text,Text,IntWritable>{

		//The count of the tweet. This is set to one because every tweet counts for one. 
		private final static IntWritable one = new IntWritable(1);
		//The text that is used as key, in this case Tweet is used. 
		private Text text = new Text("Tweet");

		/**
		 * The map function which is used to count a tweet For every tweet the text "Tweet" and the number
		 * one is written to the context.
		 */
		@SuppressWarnings("unchecked")
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//Every parsed tweet is counted by writing the number one to the context 
			context.write(text, one);

		}
	}

	/**
	 * Class that contains the reduce function of the MapReduce program. The class
	 * is an extension of the class Reducer of the Apache Hadoop API
	 */
	public static class ExampleReducer extends Reducer<Text,IntWritable,Text,IntWritable>{ 
		
		//Counter of the total number of tweets in a given data set
		private IntWritable result = new IntWritable();
		
		/**
		 * The reduce function which is used to count the total number of tweets. All the values are added together which gives
		 * the total number of tweets in a data set. 
		 */
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			//Counter of the number of tweets
			int sum = 0;
			//For all tweets the value of the tweet (in all cases 1) is added to the toal
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);
			//The key from the map function and the total number of tweets is written to the context. 
			context.write(key, result);
		}
	}

	/**
	 * Initialization of the MapReduce program
	 * @throws Exception
	 */
   public static void main(String[] args) throws Exception  {
     Configuration conf = new Configuration();
     String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
     if (otherArgs.length<2){
	System.err.println("Usage: exampleTwitter <in> [<in>....] <out>");
	System.exit(2);
     }
     Job job = new Job(conf, "Twitter Reader");
     job.setJarByClass(AllTweets.class);
     job.setMapperClass(ExampleMapper.class);
     job.setReducerClass(ExampleReducer.class);
     job.setOutputKeyClass(Text.class);
     job.setOutputValueClass(IntWritable.class);
     
     for (int i = 0; i<otherArgs.length -1; i++)  {
	FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
     }
     FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
     System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}
