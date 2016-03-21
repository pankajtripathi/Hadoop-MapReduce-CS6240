package com.assignment.a7;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Shakti Patro
 * @author Pankaj Tripathi
 * @author Kartik Mahaley
 * @author Chen Bai
 * 
 * Functionality :
 * 1. Flight Object used for transfering data across mapreduce job 
 * 2. In contrast to AirlineDetails object, contains only relevant fields. 
 * 3. compareTo() method is overriden, which takes only scheduled time into consideration
 * 		This makes the list of FlightObjects sorted by scheduled Time.
 */
public class FlightObject implements WritableComparable<FlightObject> {

	public Boolean type;
	public String origin;
	public String dest;
	public String flNum;
	public long scheduledTime;
	public long actualTime;
	public long scheduledTime2;
	public int crselapsedtime;
	public short distance;
	public short dayOfMonth;
	public short dayOfWeek;

	public FlightObject() {
	}

	public FlightObject(Boolean type, String origin, String dest, String flNum, long scheduledTime, long actualTime,
			long scheduledTime2, int crselapsedtime ,short distance, short dayOfMonth, short dayOfWeek) {
		this.type = type;
		this.origin=origin;
		this.dest=dest;
		this.flNum=flNum;
		this.scheduledTime = scheduledTime;
		this.actualTime = actualTime;
		this.scheduledTime2 = scheduledTime2;
		this.crselapsedtime = crselapsedtime;
		this.distance=distance;
		this.dayOfMonth=dayOfMonth;
		this.dayOfWeek=dayOfWeek;
	}

	@Override
	public int compareTo(FlightObject fo) {
		// compare the 2 objects based on scheduledTime
		long flightScheduledTime = fo.scheduledTime;
		long diff = this.scheduledTime - flightScheduledTime;
		if (diff < 0) return -1;
		if (diff == 0) return 0;
		return 1;
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeBoolean(type);
		dataOutput.writeUTF(origin);
		dataOutput.writeUTF(dest);
		dataOutput.writeUTF(flNum);
		dataOutput.writeLong(scheduledTime);
		dataOutput.writeLong(actualTime);
		dataOutput.writeLong(scheduledTime2);
		dataOutput.writeInt(crselapsedtime);
		dataOutput.writeShort(distance);
		dataOutput.writeShort(dayOfMonth);
		dataOutput.writeShort(dayOfWeek);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		type = dataInput.readBoolean();
		origin=dataInput.readUTF();
		dest=dataInput.readUTF();
		flNum=dataInput.readUTF();
		scheduledTime = dataInput.readLong();
		actualTime = dataInput.readLong();
		scheduledTime2 = dataInput.readLong();
		crselapsedtime = dataInput.readInt();
		distance = dataInput.readShort();
		dayOfMonth = dataInput.readShort();
		dayOfWeek = dataInput.readShort();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ (int) (scheduledTime2 ^ (scheduledTime2 >>> 32));
		result = prime * result
				+ (int) (actualTime ^ (actualTime >>> 32));
		result = prime * result + crselapsedtime;
		result = prime * result + dayOfMonth;
		result = prime * result + dayOfWeek;
		result = prime * result + ((dest == null) ? 0 : dest.hashCode());
		result = prime * result + distance;
		result = prime * result + ((flNum == null) ? 0 : flNum.hashCode());
		result = prime * result + ((origin == null) ? 0 : origin.hashCode());
		result = prime * result
				+ (int) (scheduledTime ^ (scheduledTime >>> 32));
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
		FlightObject other = (FlightObject) obj;
		if (scheduledTime2 != other.scheduledTime2)
			return false;
		if (actualTime != other.actualTime)
			return false;
		if (crselapsedtime != other.crselapsedtime)
			return false;
		if (dayOfMonth != other.dayOfMonth)
			return false;
		if (dayOfWeek != other.dayOfWeek)
			return false;
		if (dest == null) {
			if (other.dest != null)
				return false;
		} else if (!dest.equals(other.dest))
			return false;
		if (distance != other.distance)
			return false;
		if (flNum == null) {
			if (other.flNum != null)
				return false;
		} else if (!flNum.equals(other.flNum))
			return false;
		if (origin == null) {
			if (other.origin != null)
				return false;
		} else if (!origin.equals(other.origin))
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