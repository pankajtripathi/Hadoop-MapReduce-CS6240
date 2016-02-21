package com.assignment.A5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CompositeGroupKey implements WritableComparable<CompositeGroupKey> {
	String name;
	String year;
	public CompositeGroupKey(){
		
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
	public int compareTo(CompositeGroupKey t) {
	    int cmp = this.name.compareTo(t.name);
	    if (cmp != 0) {
	        return cmp;
	    }
	    return this.year.compareTo(t.year);
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
	    if (this.name != other.name && (this.name == null || !this.name.equals(other.name))) {
	        return false;
	    }
	    if (this.year != other.year && (this.year == null || !this.year.equals(other.year))) {
	        return false;
	    }
	    return true;
	}
	@Override
	public int hashCode() {
	    return this.name.hashCode() * 163 + this.year.hashCode();
	}

	@Override
	public String toString() {
		return name+ "\t" + year;
	}
}
