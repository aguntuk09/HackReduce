package org.hackreduce.amazonreviews;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.mappers.AmazonReviewMapper;
import org.hackreduce.mappers.ModelMapper;
import org.hackreduce.models.AmazonReviewRecord;

public class LinearRegressionMapper  extends org.hackreduce.examples.RecordCounter {
	
	public static class RecordCounterMapper extends AmazonReviewMapper<Text, ReviewTuple> {

		private static Calendar cal = Calendar.getInstance();

		@Override
		protected void map(AmazonReviewRecord record, Context context) throws IOException, InterruptedException {
			if (record.getReviewDate() != null) 
			{
				cal.setTime(record.getReviewDate());
				context.write(new Text(record.getReviewID())
					, /*new ObjectWritable(*/ new ReviewTuple(record.getReviewDate().getTime(),
							record.getRating()
							) /*)*/);
			}
		}
		
	};
	
	public static class Struct implements Comparable<Struct> {
		public long date;
		public float rating;
		
		public Struct(long date, float rating) {
			this.date = date;
			this.rating = rating;
		}

		@Override
		public int compareTo(Struct o) {
			return Long.valueOf(date).compareTo(o.date);
		}
	}
	
	public static class RatingAverageReducer extends Reducer<Text, ReviewTuple, Text, DoubleWritable> {

		@Override
		protected void reduce(Text key, Iterable<ReviewTuple> values, Context context) throws IOException, InterruptedException {

			List<Struct> reviewTuples = new ArrayList<Struct>();

			for(ReviewTuple tuple : values) {
				reviewTuples.add(new Struct(
						tuple.getDate(), tuple.getRating()
						));
			}
//			System.out.println(reviewTuples);
			Collections.sort(reviewTuples);
			double [] x = new double[reviewTuples.size()];
			double [] y = new double[reviewTuples.size()];
			
		    // first pass: read in data, compute xbar and ybar
			int n = 0;
			for(Struct struct : reviewTuples) {
				x[n] = struct.date;
				y[n] = struct.rating;
//				System.out.println(x[n]);
//				System.out.println(y[n]);
	            n++;
			}
			
//			System.out.println(x);
//			System.out.println(y);
			
			LinearRegression lreg = new LinearRegression(x, y);
//			System.out.println( lreg.getModel() ); 
			
			context.write(key, new DoubleWritable(lreg.getSlope()));
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
		job.setMapOutputValueClass(ReviewTuple.class);

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
		int result = ToolRunner.run(new Configuration(), new LinearRegressionMapper(), args);
		System.exit(result);
	}
}
