package com.assignment.A5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
/**
 * @author Pankaj Tripathi, Kartik Mahaley
 * */
public class CompositeGroupKey implements WritableComparable<CompositeGroupKey> {
	String name;
	String year;
	String airport;
	/*String schedule;
	String actual;
	String cancel;*/
	public CompositeGroupKey(){
		
	}
	
	public CompositeGroupKey(String name, String year,String airport) {
	    this.name = name;
	    this.year = year;
	    this.airport=airport;
	    /*this.schedule=schedule;
	    this.actual=actual;
	    this.cancel=cancel;*/
	}
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeUTF(year);
		out.writeUTF(airport);
		/*out.writeUTF(schedule);
		out.writeUTF(actual);
		out.writeUTF(cancel);*/
	}

	public void readFields(DataInput in) throws IOException {
		name = in.readUTF();
		year = in.readUTF();
		airport = in.readUTF();
		/*schedule = in.readUTF();
		actual = in.readUTF();
		cancel = in.readUTF();*/
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((year == null) ? 0 : year.hashCode());
		result = prime * result + ((airport == null) ? 0 : airport.hashCode());
		/*result = prime * result + ((arrival == null) ? 0 : arrival.hashCode());
		result = prime * result + ((cancel == null) ? 0 : cancel.hashCode());
		result = prime * result + ((schedule == null) ? 0 : schedule.hashCode());*/
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
		/*if (actual == null) {
			if (other.actual != null)
				return false;
		} else if (!actual.equals(other.actual))
			return false;
		if (arrival == null) {
			if (other.arrival != null)
				return false;
		} else if (!arrival.equals(other.arrival))
			return false;
		if (cancel == null) {
			if (other.cancel != null)
				return false;
		} else if (!cancel.equals(other.cancel))
			return false;*/
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
		if (airport == null) {
			if (other.airport != null)
				return false;
		} else if (!airport.equals(other.airport))
			return false;
		return true;
	}

	
	@Override
	public String toString() {
		return  name + "\t" + year +"\t"+airport;
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
		return this.year.compareTo(t.year);
	}
}
