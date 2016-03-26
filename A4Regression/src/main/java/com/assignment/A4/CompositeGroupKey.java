package com.assignment.A4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CompositeGroupKey implements WritableComparable<CompositeGroupKey> {
	String name;
	String year;

	public CompositeGroupKey() {

	}

	public CompositeGroupKey(String name, String year) {
		this.name = name;
		this.year = year;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeUTF(year);
	}

	public void readFields(DataInput in) throws IOException {
		name = in.readUTF();
		year = in.readUTF();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
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
		if (year == null) {
			if (other.year != null)
				return false;
		} else if (!year.equals(other.year))
			return false;
		return true;
	}

	public int compareTo(CompositeGroupKey t) {
		int cmp = this.name.compareTo(t.year);
		if (cmp != 0) {
			return cmp;
		}
		return cmp;
	}

	@Override
	public String toString() {
		return name + "\t" + year;
	}
}
