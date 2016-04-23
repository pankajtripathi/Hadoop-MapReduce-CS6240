package com.mr.io;

import java.io.Serializable;

/**
 * Implementation of a key-value pair to be used in the map-reduce
 * algorithm. For simplicity, both keys and values are taken as
 * Strings.
 * @author Shakti Patro
 * @version 1.0
 *
 */
public class Tuple<KEY extends Comparable<KEY>,VALUE> implements Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private KEY m_key = null;
	private VALUE m_value = null;
	
	/**
	 * Create an empty tuple
	 */
	public Tuple()
	{
		super();
	}
	
	/**
	 * Create a tuple with given key and value
	 * @param key The key
	 * @param value The value
	 */
	public Tuple(KEY key, VALUE value)
	{
		this();
		setKey(key);
		setValue(value);
	}
	
	/**
	 * Set the key for the tuple
	 * @param key Value of the key
	 */
	public void setKey(KEY key)
	{
		if (key == null)
			m_key = null;
		else
			m_key = key;
	}
	
	/**
	 * Get the tuple's key
	 * @return The tuple's key
	 */
	public KEY getKey()
	{
		return m_key;
	}
	
	/**
	 * Get the tuple's value
	 * @return The tuple's value
	 */
	public VALUE getValue()
	{
		return m_value;
	}
	
	@Override
	public boolean equals(Object o)
	{
		if (o == null)
			return false;
		assert o != null;
		if (o instanceof Tuple<?,?>)
		{
			return equals((Tuple<?,?>) o);
		}
		return false;
	}
	
	public boolean equals(Tuple<KEY,VALUE> t)
	{
		assert t != null;
		return m_key.equals(t.m_key) &&
			m_value.equals(t.m_value);
	}
	
	@Override
	public int hashCode()
	{
		return m_key.hashCode() + m_value.hashCode();
	}
	
	/**
	 * Set the tuple's value
	 * @param value The value's value (!)
	 */
	public void setValue(VALUE value)
	{
		if (value == null)
			m_value = null;
		else
			m_value = value;
	}
	
	@Override
	public String toString()
	{
		return "\u2329" + m_key + "," + m_value + "\u232A";
	}
	
	public int compareTo(Tuple<KEY, VALUE> o) {
		return this.getKey().compareTo(o.getKey());
	}
}