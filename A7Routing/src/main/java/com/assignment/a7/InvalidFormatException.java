package com.assignment.a7;

/*
 * @author : Shakti Patro, Pankaj Tripathi, Kartik Mahaley
 * Class Name : InvalidFormatException.java
 * Purpose : Exception class for inputs that have invalid formats.
 * Examples : Incorrect number of columns, incorrect format for particular column. 
 * 
 */
public class InvalidFormatException extends Exception {

	private static final long serialVersionUID = 12323232323L;

	public InvalidFormatException(String message) {
		super(message);
	}
}
