package org.hackreduce.amazonreviews;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.mappers.AmazonReviewMapper;
import org.hackreduce.mappers.ModelMapper;
import org.hackreduce.models.AmazonReviewRecord;

public class RecordCounter  extends org.hackreduce.examples.RecordCounter {
	public static class RecordCounterMapper extends AmazonReviewMapper<Text, LongWritable> {

		@Override
		protected void map(AmazonReviewRecord record, Context context) throws IOException, InterruptedException {
			// do something.
		}
		
	};
	@Override
	public void configureJob(Job job) {
		AmazonReviewMapper.configureJob(job);
	}

	@Override
	public Class<? extends ModelMapper<?, ?, ?, ?, ?>> getMapper() {
		return RecordCounterMapper.class;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new RecordCounter(), args);
		System.exit(result);
	}
}
