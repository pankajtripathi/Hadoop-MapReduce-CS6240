package com.mapreduce.prediction;

import java.io.DataInput;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
/**
 * @author Pankaj Tripathi, Kartik Mahaley
 * */
public class CompositeGroupKey implements WritableComparable<CompositeGroupKey> {
	String year;
	String month;
	public CompositeGroupKey(){

	}
	public CompositeGroupKey(String year, String month) {
		this.year=year;
		this.month = month;
	}
	public void write(DataOutput out) throws IOException {
		out.writeUTF(year);
		out.writeUTF(month);
	}

	public void readFields(DataInput in) throws IOException {
		year = in.readUTF();
		month = in.readUTF();
	}

	@Override
	public int compareTo(CompositeGroupKey t) {
		int cmp = this.year.compareTo(t.year);
		if (cmp != 0) {
			return cmp;
		}
		return this.month.compareTo(t.month);
	}    
	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final CompositeGroupKey other = (CompositeGroupKey) obj;
		if (this.year != other.year && (this.year == null || !this.year.equals(other.year))) {
			return false;
		}
		if (this.month != other.month && (this.month == null || !this.month.equals(other.month))) {
			return false;
		}
		return true;
	}
	@Override
	public int hashCode() {
		return this.year.hashCode() * 163 + this.month.hashCode();
	}

	@Override
	public String toString() {
		return year+"\t"+ month;
	}
}
