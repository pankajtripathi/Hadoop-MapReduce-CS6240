package com.assignment.A4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CompositeGroupKey implements WritableComparable<CompositeGroupKey> {
	String name;
	String year;
	String time;

	public CompositeGroupKey() {

	}

	public CompositeGroupKey(String name, String year, String time) {
		this.name = name;
		this.year = year;
		this.time = time;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeUTF(year);
		out.writeUTF(time);
	}

	public void readFields(DataInput in) throws IOException {
		name = in.readUTF();
		year = in.readUTF();
		time = in.readUTF();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((time == null) ? 0 : time.hashCode());
		result = prime * result + ((year == null) ? 0 : year.hashCode());
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
		if (time == null) {
			if (other.time != null)
				return false;
		} else if (!time.equals(other.time))
			return false;
		if (year == null) {
			if (other.year != null)
				return false;
		} else if (!year.equals(other.year))
			return false;
		return true;
	}

	@Override
	public int compareTo(CompositeGroupKey t) {
		int cmp = this.name.compareTo(t.name);
		if (cmp != 0) {
			return cmp;
		}
		int cmp1 = this.year.compareTo(t.year);
		if (cmp1 != 0) {
			return cmp1;
		}
		return this.time.compareTo(t.time);
	}
	
	@Override
	public String toString() {
		return name + "\t" + year + "\t" + time;
	}


}
