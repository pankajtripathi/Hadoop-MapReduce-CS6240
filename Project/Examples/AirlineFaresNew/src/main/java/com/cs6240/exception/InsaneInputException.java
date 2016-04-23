package com.cs6240.exception;

/*
 * @author : Shakti Patro
 * Class Name : InsaneInputException.java
 * Purpose : Exception class for inputs that fail the sanity test
 * Following Exceptions fall into this exception class (are considered insane):
 * 		CRSArrTime and CRSDepTime should not be zero
 * 		for timeZone = CRSArrTime - CRSDepTime - CRSElapsedTime;
 * 			timeZone % 60 should be 0
 * 		AirportID,  AirportSeqID, CityMarketID, StateFips, Wac should be larger than 0
 * 		Origin, Destination,  CityName, State, StateName should not be empty
 * 		For flights that not Cancelled:
 * 			ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
 * 		if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
 * 		if ArrDelay < 0 then ArrDelayMinutes should be zero
 * 		if ArrDelayMinutes >= 15 then ArrDel15 should be true   
 * 
 */
public class InsaneInputException extends Exception {

	private static final long serialVersionUID = 78764283462783648L;

	public InsaneInputException(String message) {
		super(message);
	}
}
