package com.cs6240.airline;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/*
 * Class: To generate composite key for mean and median calculations
 * @author: Shakti Patro , Kavyashree nagendrakumar
 */

public class CompositeGroupKey implements Comparable<CompositeGroupKey> {
	String airlinecode;
	String month;

    
    /*
     * Constructor: No arguements passed
     */
	public CompositeGroupKey() {
	}

    
    /*
     * Constructor: to set month and airline carrier code
     */
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

    //Compares two composite keys to check for airline code equality
	@Override
	public int compareTo(CompositeGroupKey t) {
		int cmp = this.airlinecode.compareTo(t.airlinecode);
		if (cmp != 0) {
			return cmp;
		}
		return this.month.compareTo(t.month);
	}

     //Method to check equality
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
