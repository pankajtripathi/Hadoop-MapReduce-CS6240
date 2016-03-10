package com.assignment.A5;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * Idea of making object is taken from Karthik Chandrana
 */
public class flightObject implements WritableComparable<flightObject> {

	
	public Boolean type;
	public long scheduledTime;
	public long actualTime;
	public short cancel;

	public flightObject() {
	}

	public flightObject(Boolean type, long scheduledTime, long actualTime, short cancel) {
		this.type = type;
		this.scheduledTime = scheduledTime;
		this.actualTime = actualTime;
		this.cancel = cancel;
	}

	@Override
	public int compareTo(flightObject fo) {
		// compare the 2 objects based on scheduledTime
		long flightScheduledTime = fo.scheduledTime;

		long diff = this.scheduledTime - flightScheduledTime;

		if (diff < 0)
			return -1;
		if (diff == 0)
			return 0;
		return 1;
	}

	@Override
	public String toString() {
		return "flightObject{" + "type=" + type + ", scheduledTime=" + scheduledTime + ", actualTime=" + actualTime
				+ ", cancel=" + cancel + '}';
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeBoolean(type);
		dataOutput.writeLong(scheduledTime);
		dataOutput.writeLong(actualTime);
		dataOutput.writeShort(cancel);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		type = dataInput.readBoolean();
		scheduledTime = dataInput.readLong();
		actualTime = dataInput.readLong();
		cancel = dataInput.readShort();
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (actualTime ^ (actualTime >>> 32));
		result = prime * result + cancel;
		result = prime * result + (int) (scheduledTime ^ (scheduledTime >>> 32));
		result = prime * result + ((type == null) ? 0 : type.hashCode());
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
		flightObject other = (flightObject) obj;
		if (actualTime != other.actualTime)
			return false;
		if (cancel != other.cancel)
			return false;
		if (scheduledTime != other.scheduledTime)
			return false;
		if (type == null) {
			if (other.type != null)
				return false;
		} else if (!type.equals(other.type))
			return false;
		return true;
	}


}