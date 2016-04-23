package com.cs6240.exception;

/*
 * @author : Shakti Patro
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
