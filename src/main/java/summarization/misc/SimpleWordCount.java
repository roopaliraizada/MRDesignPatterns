package summarization.misc;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SimpleWordCount {
	public static class WordCountMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private Text word = new Text();
		private static final IntWritable one = new IntWritable(1);

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			StringTokenizer st = new StringTokenizer(value.toString());
			while (st.hasMoreTokens()) {
				word.set(st.nextToken());
				context.write(word, one);
			}
		}

	}

	public static class WordCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		IntWritable result = new IntWritable();

		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable value : values) {
				count += value.get();
			}
			result.set(count);
			context.write(key, result);
		}
	}

	public static void main(String args[]) {
		Configuration conf = new Configuration();
		Job job;

		try {
			job = Job.getInstance(conf, "simple word count");

			job.setJarByClass(SimpleWordCount.class);
			job.setMapperClass(WordCountMapper.class);
			// job.setCombinerClass(WordCountReducer.class);
			job.setReducerClass(WordCountReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
