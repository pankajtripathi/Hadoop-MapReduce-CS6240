package com.assignment.A4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * This is class for a custom value for the map output value and hence the reduce input value  
 * @author prasadmemane
 * @author swapnilmahajan
 */
public class PriceTime implements WritableComparable<PriceTime> {

	private DoubleWritable price;
	private DoubleWritable time;
	
	public PriceTime(String price, String time) {
		set(new DoubleWritable(Double.valueOf(price)), new DoubleWritable(Double.valueOf(time))); 
	}
	
	public PriceTime() {
		set(new DoubleWritable(), new DoubleWritable());
	}
	
	private void set(DoubleWritable price, DoubleWritable time) {
		this.price = price;
		this.time = time;
	}

	public void readFields(DataInput in) throws IOException {
		price.readFields(in);
		time.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		price.write(out);
		time.write(out);
	}

	public DoubleWritable getPrice() {
		return price;
	}

	public void setPrice(DoubleWritable price) {
		this.price = price;
	}

	public DoubleWritable getTime() {
		return time;
	}

	public void setTime(DoubleWritable time) {
		this.time = time;
	}
	
	@Override
	public String toString() {
		return price + "\t" + time;
	}

	public int compareTo(PriceTime o) {
		return 0;
	}
	
}

