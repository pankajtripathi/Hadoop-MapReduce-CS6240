package com.mr.io;

import java.util.Iterator;

/**
 * Data source used as the input of the map and reduce 
 * phases. An InCollector can be used to enumerate data
 * tuples using the hasNext() and next() methods, like an Iterator
 * 
 * @author Shakti Patro
 * @version 1.0
 *
 */
public interface InCollector<KEY extends Comparable<KEY>,VALUE> extends Iterator<Tuple<KEY,VALUE>>
{
	/**
	 * Count the number of tuples in the collector
	 * @return The number of tuples. A Collector for which
	 * the size cannot be computed should return -1.
	 */
	public int count();
	
	/**
	 * Rewinds the collector to the beginning of its enumeration
	 */
	public void rewind();
}