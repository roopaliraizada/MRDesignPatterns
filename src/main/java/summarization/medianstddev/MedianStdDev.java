package summarization.medianstddev;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import summarization.util.MRDPUtils;

public class MedianStdDev {

	/*
	 * Mapper
	 */
	public static class MedianStdDevMapper extends
			Mapper<Object, Text, IntWritable, SortedMapWritable> {

		private IntWritable commentLength = new IntWritable();
		private static final LongWritable ONE = new LongWritable(1);
		private IntWritable outHour = new IntWritable();

		private final static SimpleDateFormat frmt = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss.SSS");

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());

			// Grab the "CreationDate" field,
			// since it is what we are grouping by
			String strDate = parsed.get("CreationDate");

			// Grab the comment to find the length
			String text = parsed.get("Text");

			// Get the hour this comment was posted in
			Date creationDate;
			try {
				creationDate = frmt.parse(strDate);

				outHour.set(creationDate.getHours());
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			commentLength.set(text.length());
			SortedMapWritable outCommentLength = new SortedMapWritable();
			outCommentLength.put(commentLength, ONE);

			// Write out the user ID with min max dates and count
			context.write(outHour, outCommentLength);
		}
	}

	/*
	 * Combiner
	 */
	public static class MedianStdDevCombiner
			extends
			Reducer<IntWritable, SortedMapWritable, IntWritable, SortedMapWritable> {

		protected void reduce(IntWritable key,
				Iterable<SortedMapWritable> values, Context context)
				throws IOException, InterruptedException {

			SortedMapWritable outValue = new SortedMapWritable();

			for (SortedMapWritable v : values) {
				for (Entry<WritableComparable, Writable> entry : v.entrySet()) {
					LongWritable count = (LongWritable) outValue.get(entry
							.getKey());

					if (count != null) {
						count.set(count.get()
								+ ((LongWritable) entry.getValue()).get());
					} else {
						outValue.put(entry.getKey(), new LongWritable(
								((LongWritable) entry.getValue()).get()));
					}
				}
			}
			context.write(key, outValue);
		}
	}

		/*
		 * Reducer
		 */
		public static class MedianStdDevReducer
				extends
				Reducer<IntWritable, SortedMapWritable, IntWritable, MedianStdDevTuple> {

			private MedianStdDevTuple result = new MedianStdDevTuple();
			private TreeMap<Integer, Long> commentLengthCounts = new TreeMap<Integer, Long>();

			public void reduce(IntWritable key,
					Iterable<SortedMapWritable> values, Context context)
					throws IOException, InterruptedException {

				float sum = 0;
				long totalComments = 0;
				commentLengthCounts.clear();
				result.setMedian(0);
				result.setStdDev(0);
				for (SortedMapWritable v : values) {
					for (Entry<WritableComparable, Writable> entry : v
							.entrySet()) {
						int length = ((IntWritable) entry.getKey()).get();
						long count = ((LongWritable) entry.getValue()).get();

						totalComments += count;
						sum += length * count;

						Long storedCount = commentLengthCounts.get(length);
						if (storedCount == null) {
							commentLengthCounts.put(length, count);
						} else {
							commentLengthCounts
									.put(length, storedCount + count);
						}
					}
				}
				long medianIndex = totalComments / 2L;
				long previousComments = 0;
				long comments = 0;
				int prevKey = 0;
				for (Entry<Integer, Long> entry : commentLengthCounts
						.entrySet()) {
					comments = previousComments + entry.getValue();

					if ((previousComments <= medianIndex)
							&& (medianIndex < comments)) {
						if (totalComments % 2 == 0
								&& previousComments == medianIndex) {
							result.setMedian((float) (entry.getKey() + prevKey) / 2.0f);
						} else {
							result.setMedian(entry.getKey());
						}
						break;
					}

					previousComments = comments;
					prevKey = entry.getKey();
				}
				// calculate standard deviation
				float mean = sum / totalComments;

				float sumOfSquares = 0.0f;
				for (Entry<Integer, Long> entry : commentLengthCounts
						.entrySet()) {
					sumOfSquares += (entry.getKey() - mean)
							* (entry.getKey() - mean) * entry.getValue();
				}

				result.setStdDev((float) Math.sqrt(sumOfSquares
						/ (totalComments - 1)));
				context.write(key, result);
			}

		}
	
	
	/**
	 * Usage: bin/hadoop jar /Users/rraizada/Documents/eclipse_workspaces/MRP/MRDesignPatterns/target/MRDesignPatterns-0.0.1-SNAPSHOT.jar summarization.medianstddev.MedianStdDev /user/rraizada/data/input/sample.xml /user/rraizada/data/output_MedianStdDev
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage MedianStdDev input output");
			System.exit(2);
		}

		Job job = new Job(conf, "Optimized Summarization: Median Standard Deviation");
		job.setJarByClass(MedianStdDev.class);
		job.setMapperClass(MedianStdDevMapper.class);
		job.setCombinerClass(MedianStdDevCombiner.class);
		job.setReducerClass(MedianStdDevReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(MedianStdDevTuple.class);

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