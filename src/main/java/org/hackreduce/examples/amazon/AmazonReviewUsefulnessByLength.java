package org.hackreduce.examples.amazon;

import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.mappers.AmazonReviewMapper;
import org.hackreduce.mappers.ModelMapper;
import org.hackreduce.models.AmazonReviewRecord;

public class AmazonReviewUsefulnessByLength  extends org.hackreduce.examples.RecordCounter {
	public static class RecordCounterMapper extends AmazonReviewMapper<Text, FloatWritable> {

		private static Calendar cal = Calendar.getInstance();

		@Override
		protected void map(AmazonReviewRecord record, Context context) throws IOException, InterruptedException {
			if (record.getReviewDate() == null || record.getTotalCount() == 0 || record.getText() == null) return;
			cal.setTime(record.getReviewDate());
			context.write(new Text("" + (record.getText().length() / 10) )
				, new FloatWritable( (float) record.getUsefulCount() / record.getTotalCount() ));
		}
		
	};
	
	public static class RatingAverageReducer extends Reducer<Text, FloatWritable, Text, DoubleWritable> {

		@Override
		protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
			long count = 0;
			double total = 0.0;
			for(FloatWritable rating : values) {
				total += rating.get();
				count++;
			}
			double average = total / count; 

			context.write(key, new DoubleWritable(average));
		}

	}

	
	@Override
	public void configureJob(Job job) {
		AmazonReviewMapper.configureJob(job);
	}

	@Override
	public Class<? extends ModelMapper<?, ?, ?, ?, ?>> getMapper() {
		return RecordCounterMapper.class;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

        if (args.length != 2) {
        	System.err.println("Usage: " + getClass().getName() + " <input> <output>");
        	System.exit(2);
        }

        // Creating the MapReduce job (configuration) object
        Job job = new Job(conf);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        // Tell the job which Mapper and Reducer to use (classes defined above)
        job.setMapperClass(getMapper());
		job.setReducerClass(RatingAverageReducer.class);

        configureJob(job);

		// This is what the Mapper will be outputting to the Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);

		// This is what the Reducer will be outputting
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// Setting the input folder of the job 
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// Preparing the output folder by first deleting it if it exists
        Path output = new Path(args[1]);
        FileSystem.get(conf).delete(output, true);
	    FileOutputFormat.setOutputPath(job, output);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new AmazonReviewUsefulnessByLength(), args);
		System.exit(result);
	}
}
