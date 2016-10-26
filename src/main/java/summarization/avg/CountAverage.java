package summarization.avg;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import summarization.util.MRDPUtils;
/*
 * Given a list of user comments, determine the average comment length per hour of day.
 */
public class CountAverage {

	public static class AverageMapper extends
			Mapper<Object, Text, IntWritable, CountAverageTuple> {
		IntWritable outHour = new IntWritable();
		CountAverageTuple outCountAverage = new CountAverageTuple();
		SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

		@Override
		protected void map(Object key, Text value, Context context) {
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());
			String strdate = parsed.get("CreationDate");
			Date creationDate;
			try {
				creationDate = fmt.parse(strdate);

				outHour.set(creationDate.getHours());

				String comment = parsed.get("Text");
				outCountAverage.setCount(1);
				outCountAverage.setAverage(comment.length());

				context.write(outHour, outCountAverage);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
	
	public static class AverageReducer extends Reducer<IntWritable,CountAverageTuple, IntWritable, CountAverageTuple>{
		float count = 0;
		float length = 0;
		private CountAverageTuple result = new CountAverageTuple();
		@Override
		protected void reduce(IntWritable key, Iterable<CountAverageTuple> values, Context context){
			// sum of lengths / totalCount
			
			for(CountAverageTuple value : values){
				count += value.getCount();
				length += value.getAverage() * value.getCount();
			}
			result.setCount(count);
			result.setAverage(length/count);
			
			try {
				context.write(key, result);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args){
		Configuration conf = new Configuration();
		String[] otherArgs;
		try {
			otherArgs = new GenericOptionsParser(conf, args)
					.getRemainingArgs();
			
			if (otherArgs.length < 2) {
				System.err.println("Usage CountAverage input output");
				System.exit(2);
			}
			
			Job job = new Job(conf, "Count Average Job");
			job.setJarByClass(CountAverage.class);
			job.setMapperClass(AverageMapper.class);
			job.setReducerClass(AverageReducer.class);
			job.setCombinerClass(AverageReducer.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(CountAverageTuple.class);
			
			FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			
			boolean result = job.waitForCompletion(true);
			if (result) {
				System.exit(0);
			} else {
				System.exit(1);
			}
		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
