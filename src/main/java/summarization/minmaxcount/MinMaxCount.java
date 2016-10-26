package summarization.minmaxcount;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import summarization.util.MRDPUtils;

/**
 * Given a list of user comments, determine the first and last time the user
 * commented and the total number of comments from that user.
 * */
public class MinMaxCount {

	public static class MinMaxCountMapper extends
			Mapper<Object, Text, Text, MinMaxCountTuple> {

		// Our output key and value Writables
		private Text outuserId = new Text();
		private MinMaxCountTuple outTuple = new MinMaxCountTuple();

		 // This object will format the creation date string into a Date object
		private final static SimpleDateFormat sdf = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss.SSSS");

		@Override
		protected void map(Object key, Text value,
				Context context)
				throws IOException, InterruptedException {

			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());
			// Grab the "CreationDate" field since it is what we are finding
	        // the min and max value of
			String date = parsed.get("CreationDate");
			
			// Grab the “UserID” since it is what we are grouping by
			String userId = parsed.get("UserId");

			try {
				// Parse the string into a Date object
				Date creationDate = sdf.parse(date);
				
				// Set the minimum and maximum date values to the creationDate
				outTuple.setMin(creationDate);
				outTuple.setMax(creationDate);
			} catch (java.text.ParseException e) {
				System.err.println("Unable to parse Date in XML");
				System.exit(3);
			}
			// Set the comment count to 1
			outTuple.setCount(1);
			// Set our user ID as the output key
			outuserId.set(userId);
			// Write out the hour and the average comment length
			context.write(outuserId, outTuple);

		}

	}

	public static class MinMaxCountReducer extends
			Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {
		// Our output value Writable
		private MinMaxCountTuple result = new MinMaxCountTuple();

		protected void reduce(Text userId, Iterable<MinMaxCountTuple> values,
				 Context context)
				throws IOException, InterruptedException {
			// Initialize our result
			result.setMin(null);
			result.setMax(null);
			result.setCount(0);
			int sum = 0;
			
			//int count = 0;
			for (MinMaxCountTuple tuple : values) {
				// If the value's min is less than the result's min
	            // Set the result's min to value's
				if (result.getMin() == null
						|| tuple.getMin().compareTo(result.getMin()) < 0) {
					result.setMin(tuple.getMin());
				}

				// If the value's max is more than the result's max
	            // Set the result's max to value's
				if (result.getMax() == null
						|| tuple.getMax().compareTo(result.getMax()) > 0) {
					result.setMax(tuple.getMax());
				}

				//System.err.println(count++);
				// Add to our sum the count for value
				sum += tuple.getCount();
			}
			// Set our count to the number of input values
			result.setCount(sum);
			context.write(userId, result);
		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage MinMaxCount input output");
			System.exit(2);
		}

		Job job = new Job(conf, "Summarization min max count");
		job.setJarByClass(MinMaxCount.class);
		job.setMapperClass(MinMaxCountMapper.class);
		//Since counting is commutative & associative, reducer can be safely used as combiner
		// job.setCombinerClass(MinMaxCountReducer.class);
		job.setReducerClass(MinMaxCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MinMaxCountTuple.class);

		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		boolean result = job.waitForCompletion(true);
		if (result) {
			System.exit(0);
		} else {
			System.exit(1);
		}

	}
}
