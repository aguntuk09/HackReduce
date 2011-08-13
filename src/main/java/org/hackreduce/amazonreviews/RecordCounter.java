package org.hackreduce.amazonreviews;

import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.mappers.AmazonReviewMapper;
import org.hackreduce.mappers.ModelMapper;
import org.hackreduce.models.AmazonReviewRecord;

public class RecordCounter  extends org.hackreduce.examples.RecordCounter {
	public static class RecordCounterMapper extends AmazonReviewMapper<Text, FloatWritable> {

		private static Calendar cal = Calendar.getInstance();

		@Override
		protected void map(AmazonReviewRecord record, Context context) throws IOException, InterruptedException {
			cal.setTime(record.getReviewDate());
			context.write(new Text(Integer.toString(cal.get(Calendar.DAY_OF_YEAR)))
				, new FloatWritable( record.getRating() ));
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
