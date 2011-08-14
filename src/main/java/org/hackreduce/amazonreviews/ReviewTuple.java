package org.hackreduce.amazonreviews;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ReviewTuple implements Writable, Comparable<ReviewTuple> {
	private long reviewDate;
	private float rating;

	public ReviewTuple() {}
	
	public ReviewTuple(long date, float rating) {
		this.reviewDate = date;
		this.rating = rating;
	}
	
	public long getDate() { return reviewDate; }
	public float getRating() { return rating; }

	@Override
	public void readFields(DataInput in) throws IOException {
		rating = in.readFloat();
		reviewDate = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(rating);
		out.writeLong(reviewDate);
	}
	
	public static ReviewTuple read(DataInput in) throws IOException {
        ReviewTuple w = new ReviewTuple();
        w.readFields(in);
        return w;
      }

	public String toString() {
		return reviewDate + " " + rating;
	}

	@Override
	public int compareTo(ReviewTuple o) {
		return Long.valueOf(reviewDate).compareTo(o.getDate());
	}
 }