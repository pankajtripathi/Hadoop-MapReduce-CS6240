package com.assignment.a7;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
/**
 * @author Shakti Patro
 * @author Pankaj Tripathi
 * @author Kartik Mahaley
 * @author Chen Bai
 * 
 * Functionality :
 * 1. Composite Key class 
 * 2. Implements the Writable comparable 
 * 3. compareTo() method is overriden  
 */
public class CompositeGroupKey implements WritableComparable<CompositeGroupKey> {
	String name;
	String yearMonth;
	String airport;
	public CompositeGroupKey(){

	}

	public CompositeGroupKey(String name, String year,String airport) {
		this.name = name;
		this.yearMonth = year;
		this.airport=airport;
	}
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeUTF(yearMonth);
		out.writeUTF(airport);
	}

	public void readFields(DataInput in) throws IOException {
		name = in.readUTF();
		yearMonth = in.readUTF();
		airport = in.readUTF();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((yearMonth == null) ? 0 : yearMonth.hashCode());
		result = prime * result + ((airport == null) ? 0 : airport.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CompositeGroupKey other = (CompositeGroupKey) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (yearMonth == null) {
			if (other.yearMonth != null)
				return false;
		} else if (!yearMonth.equals(other.yearMonth))
			return false;
		if (airport == null) {
			if (other.airport != null)
				return false;
		} else if (!airport.equals(other.airport))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return  name + "\t" + yearMonth +"\t"+airport;
	}

	@Override
	public int compareTo(CompositeGroupKey t) {
		int cmp = this.name.compareTo(t.name);
		if (cmp != 0) {
			return cmp;
		}
		int cmp1 = this.airport.compareTo(t.airport);
		if (cmp1 != 0) {
			return cmp1;
		}
		return this.yearMonth.compareTo(t.yearMonth);
	}
}
