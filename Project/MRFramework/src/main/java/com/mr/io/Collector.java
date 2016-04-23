package com.mr.io;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Data source used both as the input and output of the map and reduce 
 * phases. A Collector can be used to:
 * 
 * 1. Store data tuples using the collect method
 * 2. Enumerate data tuples using the hasNext() and next() methods, like an Iterator
 * 3. Partition the set of tuples into a set of Collectors, 
 * 		grouping tuples by their key, 
 * 		using the subCollector(Object) and subCollectors() methods
 * 
 * @author Shakti Patro
 * @version 1.0
 *
 */
public class Collector<KEY extends Comparable<KEY>,VALUE> implements InCollector<KEY,VALUE>, OutCollector<KEY,VALUE> , Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private List<Tuple<KEY,VALUE>> m_tuples = new LinkedList<Tuple<KEY,VALUE>>();
	private Iterator<Tuple<KEY,VALUE>> m_it = null;

	/**
	 * Return the Collector's contents as a list of tuples
	 * @return The list of tuples
	 */
	public List<Tuple<KEY,VALUE>> toList() {
		return m_tuples;
	}

	/**
	 * Add a collection of tuples to the Collector
	 * @param list A collection of {@link Tuple}
	 */
	public void addAll(Collection<Tuple<KEY,VALUE>> list) {
		synchronized (this) {
			m_tuples.addAll(list);
		}
	}

	/**
	 * Add a new tuple to the Collector in a synchronized mode
	 * @param t The {@link Tuple} to add
	 */
	public void collect(Tuple<KEY,VALUE> t) {
		synchronized (this) {
			m_tuples.add(t);
		}
	}

	/**
	 * Returns a new Collector whose content is made of all tuples with
	 * given key
	 * @param key The key to find
	 * @return A new {@link Collector}
	 */
	public Collector<KEY,VALUE> subCollector(KEY key) {
		Collector<KEY,VALUE> c = new Collector<KEY,VALUE>();
		synchronized (this) {
			for (Tuple<KEY,VALUE> t : m_tuples)
			{
				if (t.getKey().equals(key))
					c.m_tuples.add(t);
			}
		}
		return c;

	}

	public int count() {
		synchronized (this) {
			return m_tuples.size();
		}
	}

	/**
	 * Partitions the set of tuples into new collectors, each containing all
	 * tuples with the same key
	 * @return A map from keys to Collectors
	 */
	public Map<KEY,Collector<KEY,VALUE>> subCollectors() {
		Map<KEY,Collector<KEY,VALUE>> out = new HashMap<KEY,Collector<KEY,VALUE>>();

		synchronized (this) {
			for ( Tuple<KEY,VALUE> t : m_tuples)
			{
				KEY key = t.getKey();
				Collector<KEY,VALUE> c = out.get(key);

				if (c == null)
					c = new Collector<KEY,VALUE>();

				c.collect(t);
				out.put(key, c);
			}
		}
		return out;
	}

	@Override
	public boolean hasNext() {
		if (m_it == null)
			m_it = m_tuples.iterator();
		return m_it.hasNext();
	}

	@Override
	public Tuple<KEY,VALUE> next() {
		return m_it.next();
	}

	@Override
	public void remove() {
		m_it.remove();
	}

	@Override
	public String toString() {
		return m_tuples.toString();
	}

	@Override
	public void rewind() {
		m_it = null;
	}

	//	public List<KEY> getKeys(){
	//		List<KEY> keys=new ArrayList<KEY>();
	//		synchronized (this) {
	//			for ( Tuple<KEY,VALUE> t : m_tuples)
	//				keys.add(t.getKey());
	//		}
	//		return keys;
	//	}

	public void sort() {
		Collections.sort(m_tuples, new Comparator<Tuple<KEY, VALUE>>() {

			@Override
			public int compare(Tuple<KEY, VALUE> o1, Tuple<KEY, VALUE> o2) {
				return o1.getKey().compareTo(o2.getKey());
			}
		});
	}


}