package com.mr.mapreduce;

import com.mr.io.OutCollector;
import com.mr.io.Tuple;

/**
 * Interface declaration of the map phase of the map-reduce
 * algorithm.
 * @author Shakti Patro
 * @version 1.0
 *
 */
public interface Mapper<KEY extends Comparable<KEY>,VALUE>
{
	/**
	 * Map function
	 * @param c A {@link OutCollector} that will be used to write output tuples
	 * @param t A {@link Tuple} to process
	 */
	public void map(OutCollector<KEY,VALUE> c, Tuple<KEY,VALUE> t);
}