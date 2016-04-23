package com.cs6240.airline;

import org.apache.commons.lang.StringUtils;

import com.cs6240.exception.InsaneInputException;

/*
 * This class contains all files which are requierd for sanity checking of an airline data
 * author: Shakti Patro , Kavyashree nagendrakumar
 */


/*
 *class : To perform airline record validation
 */
public class AirlineSanity {

    
    /*
     * Validates all conditions to check if the record is sane. Takes AirlineDetailsPojo object as input.
     */
	public static void sanityCheck(AirlineDetailsPojo airline) throws InsaneInputException{

		//calculate timezone
		int crsArrTimeInMinutes = calculateMinutes(airline.getCrsArrivalTime());
		int crsDepTimeInMinutes = calculateMinutes(airline.getCrsDepartureTime());
		int crsElapsedTimeInMinutes = airline.getCrsElapsedTime();
		int actualArrTimeInMinutes = calculateMinutes(airline.getActualArrivalTime());
		int actualDepTimeInMinutes = calculateMinutes(airline.getActualDepartureTime());
		int actualElapsedTimeInMinutes = airline.getActualElapsedTime();
		int timezone = crsArrTimeInMinutes - crsDepTimeInMinutes - crsElapsedTimeInMinutes;
		int actulaTimezone = actualArrTimeInMinutes - 	actualDepTimeInMinutes - actualElapsedTimeInMinutes - timezone;

        //Validates CRS time
		boolean condition1 = (crsArrTimeInMinutes == 0 && crsDepTimeInMinutes == 0);
		boolean condition2 = (timezone % 60 != 0);
		boolean condition3 = (airline.getOriginAirportId() < 1 || airline.getOriginAirportSequenceId() < 1 
				|| airline.getOriginCityMarketId() < 1 || airline.getOriginStateFips() < 1 
				|| airline.getOriginWac() < 1
				|| airline.getDestinationAirportId() < 1 || airline.getDestinationAirportSequenceId() < 1 
				|| airline.getDestinationCityMarketId() < 1 || airline.getDestinationStateFips() < 1 
				|| airline.getDestinationWac() < 1);
        
         //Validates empty origin and destination data
		boolean condition4 = StringUtils.isEmpty(airline.getOrigin()) || StringUtils.isEmpty(airline.getOriginCityName())
				|| StringUtils.isEmpty(airline.getOriginStateName()) || StringUtils.isEmpty(airline.getOriginStateAbbr())
				|| StringUtils.isEmpty(airline.getDestination()) || StringUtils.isEmpty(airline.getDestinationCityName())
				|| StringUtils.isEmpty(airline.getDestinationStateName()) || StringUtils.isEmpty(airline.getDestinationStateAbbr());
		
        //validates cancelled flights
		boolean condition5 = (airline.getCancelled() == 0) && 
				(actulaTimezone % 24 != 0);
				
        //Validates time delays
		boolean condition6 = (airline.getArrivalDelay() > 0) 
				&& (airline.getArrivalDelay() != airline.getArrivalDelayMinutes());
		boolean condition7 = (airline.getArrivalDelay() < 0)  
				&& (airline.getArrivalDelayMinutes() != 0);
		boolean condition8 =(airline.getArrivalDelayMinutes() > 15)  
				&& (airline.getArrivalDelay15() == 0);
		
		if(condition1 && condition2 && condition3 && condition4 
				&& condition5 && condition6 && condition7 && condition8)
			throw new InsaneInputException("Sanity test failed");


	}

	/*
	 * This method takes a time in HHMM format and returns the minute value
	 * as HH*60 + MM
	 * Ex: 1030 returns 630.
	 */
	private static int calculateMinutes(Integer time) {
		int hours = time/100;
		int minutes = time%100;
		return hours* 60 + minutes;
	}
}
