package com.mr.mapreduce;

import com.mr.io.InCollector;
import com.mr.io.OutCollector;

/**
 * Interface declaration of the reduce phase of the map-reduce
 * algorithm.
 * @author Shakti Patro
 * @version 1.0
 *
 */
public interface Reducer<KEY extends Comparable<KEY>,VALUE>
{
	/**
	 * Reduce function
	 * @param out A {@link OutCollector} that will be used to write output tuples
	 * @param key The key associated to this instance of reducer
	 * @param in An {@link InCollector} containing all the tuples generated
	 * in the map phase for the given key
	 */
	public void reduce(OutCollector<KEY,VALUE> out, KEY key, InCollector<KEY,VALUE> in);
}