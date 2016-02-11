package com.mapreduce.assignment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/*
 * @author : Kartik Mahaley, Pankaj Tripathi
 * Class Name : CompositeGroupKey.java
 * Purpose : For a given pair of airline code and month, produces key with month code
 *  
 */
public class CompositeGroupKey implements WritableComparable<CompositeGroupKey> {
	String airlinecode;
	String month;

	public CompositeGroupKey() {
	}

	public CompositeGroupKey(String airlinecode, String month) {
		this.airlinecode = airlinecode;
		this.month = month;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(airlinecode);
		out.writeUTF(month);
	}

	public void readFields(DataInput in) throws IOException {
		airlinecode = in.readUTF();
		month = in.readUTF();
	}

	public int compareTo(CompositeGroupKey t) {
		int cmp = this.airlinecode.compareTo(t.airlinecode);
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
		if (this.airlinecode != other.airlinecode
				&& (this.airlinecode == null || !this.airlinecode.equals(other.airlinecode))) {
			return false;
		}
		if (this.month != other.month && (this.month == null || !this.month.equals(other.month))) {
			return false;
		}
		return true;
	}

	@Override
	public int hashCode() {
		return this.airlinecode.hashCode() * 163 + this.month.hashCode();
	}

	@Override
	public String toString() {
		return month + "\t" + airlinecode;
	}
}
