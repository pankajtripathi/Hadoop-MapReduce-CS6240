package com.mr.io;

/**
 * Data source used as the output of the map and reduce 
 * phases. An OutCollector can be used to
 * store data tuples using the collect method.
 *
 * @author Shakti Patro
 * @version 1.0
 *
 */
public interface OutCollector<KEY extends Comparable<KEY>,VALUE>
{
	/**
	 * Add a new tuple to the Collector
	 * @param t The {@link Tuple} to add
	 */
	public void collect(Tuple<KEY,VALUE> t);
	
	/**
	 * Rewinds the collector to the beginning of its enumeration
	 */
	public void rewind();
}