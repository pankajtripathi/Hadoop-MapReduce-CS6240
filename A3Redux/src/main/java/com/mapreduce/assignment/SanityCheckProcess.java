package com.mapreduce.assignment;

/**
 * Created on 2/5/16.
 * author: Pankaj Tripathi, Kartik Mahaley
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang.StringUtils;

import com.opencsv.CSVReader;

public class SanityCheckProcess implements Runnable {
	File csvFile = null;
	String origin= null, destination= null,originCityName= null,destCityName= null,originStateAbr= null,
			destStateAbr= null,originStateName= null,destStateName= null, cancelled= null,
			carrier= null,price= null;
	String originAirportID=null,destAirportID= null,originAirportSeqID = null,destAirportSeqID= null,
			originCityMarketID = null,destCityMarketID= null,
			originStateFips= null,destStateFips= null,
			originWac= null,destWac= null,monthStr=null,yearStr=null,year=null;
	int timeZone = 0,month=0;
	String  crsArrTime= null,crsDepTime= null,crsElapsedTime= null,arrTime= null,depTime= null,
			actualElapsedTime= null,arrDelay= null,arrDelayMinutes = null, arrDel15= null;
	public static int badData,goodData;
	public static Set<String> activeIn2015 = new TreeSet<String>();
	TreeMap<String,List<Double>> map;
	List<Double> list;
	public SanityCheckProcess(File f) {
		this.csvFile = f;
	}
	
	/**
	 * This method reads CSV file and extracts data based on the columns.
	 * Open CSV file and extract the index of the required columns.
	 * These indexes will be used to access the data from each line that is data[].
	 * */
	public void run() {
		if (csvFile.isFile()) {
			CSVReader reader = null;
			String data[];
			int crsArrTimeIdx = 0, crsDepTimeIdx = 0, originAirportIDIdx = 0, destAirportIDIdx = 0,
					originAirportSeqIDIdx = 0,destAirportSeqIDIdx = 0, originCityMarketIDIdx = 0, 
					destCityMarketIDIdx = 0,originStateFipsIdx = 0, destStateFipsIdx = 0, originWacIdx = 0, 
					destWacIdx = 0, originIdx = 0,destinationIdx = 0, originCityNameIdx = 0, destCityNameIdx = 0,
					originStateAbrIdx = 0, destStateAbrIdx = 0, originStateNameIdx = 0, destStateNameIdx = 0,
					arrTimeIdx = 0,depTimeIdx = 0, actualElapsedTimeIdx = 0, arrDelayMinIdx = 0, yearIdx = 0,
					crsElapsedTimeIdx = 0, arrDelayIdx = 0, arrDel15Idx = 0, cancelledIdx = 0, priceIdx = 0,
					carrIdx = 0, monthIdx = 0;
			map = new TreeMap<String, List<Double>>();
			try {
				FileInputStream fin=new FileInputStream(csvFile);
				GZIPInputStream gzin=new GZIPInputStream(fin);
				InputStreamReader isr=new InputStreamReader(gzin);
				BufferedReader bfr = new BufferedReader(isr);
				reader = new CSVReader(bfr);
				String col[] = reader.readNext();
				badData++;
				for (int i = 0; i < col.length; i++) {
					if (col[i].equals("CRS_ARR_TIME"))
						crsArrTimeIdx = i;
					if (col[i].equals("CRS_DEP_TIME"))
						crsArrTimeIdx = i;
					if (col[i].equals("ORIGIN_AIRPORT_ID"))
						originAirportIDIdx = i;
					if (col[i].equals("DEST_AIRPORT_ID"))
						destAirportIDIdx = i;
					if (col[i].equals("DEST_AIRPORT_SEQ_ID"))
						destAirportSeqIDIdx = i;
					if (col[i].equals("ORIGIN_AIRPORT_SEQ_ID"))
						originAirportSeqIDIdx = i;
					if (col[i].equals("ORIGIN_CITY_MARKET_ID"))
						originCityMarketIDIdx = i;
					if (col[i].equals("DEST_CITY_MARKET_ID"))
						destCityMarketIDIdx = i;
					if (col[i].equals("ORIGIN_WAC"))
						originWacIdx = i;
					if (col[i].equals("DEST_WAC"))
						destWacIdx = i;
					if (col[i].equals("ORIGIN"))
						originIdx = i;
					if (col[i].equals("DEST"))
						destinationIdx = i;
					if (col[i].equals("ORIGIN_CITY_NAME"))
						originCityNameIdx = i;
					if (col[i].equals("DEST_CITY_NAME"))
						destCityNameIdx = i;
					if (col[i].equals("ORIGIN_STATE_ABR"))
						originStateAbrIdx = i;
					if (col[i].equals("DEST_STATE_ABR"))
						destStateAbrIdx = i;
					if (col[i].equals("ORIGIN_STATE_NM"))
						originStateNameIdx = i;
					if (col[i].equals("DEST_STATE_NM"))
						destStateNameIdx = i;
					if (col[i].equals("ARR_TIME"))
						arrTimeIdx = i;
					if (col[i].equals("DEP_TIME"))
						depTimeIdx = i;
					if (col[i].equals("ACTUAL_ELAPSED_TIME"))
						actualElapsedTimeIdx = i;
					if (col[i].equals("CRS_ELAPSED_TIME"))
						crsElapsedTimeIdx = i;
					if (col[i].equals("ARR_DELAY"))
						arrDelayIdx = i;
					if (col[i].equals("ARR_DELAY_NEW"))
						arrDelayMinIdx = i;
					if (col[i].equals("ARR_DEL15"))
						arrDel15Idx = i;
					if (col[i].equals("CANCELLED"))
						cancelledIdx = i;
					if (col[i].equals("AVG_TICKET_PRICE"))
						priceIdx = i;
					if (col[i].equals("CARRIER"))
						carrIdx = i;
					if (col[i].equals("MONTH"))
						monthIdx = i;
					if (col[i].equals("YEAR"))
						yearIdx = i;
				}
				while ((data = reader.readNext()) != null) {
					/**
					 *  Check if any line has more than 110 columns. If it is the case then it means that the line is
					 *  not similar to other line so it can be considered as corrupt line
					 * */
					if (data.length < 110) {badData++;continue;}
					crsArrTime = data[crsArrTimeIdx];
					crsDepTime = data[crsDepTimeIdx];
					crsElapsedTime = data[crsElapsedTimeIdx];
					originAirportID = data[originAirportIDIdx];
					destAirportID = data[destAirportIDIdx];
					originAirportSeqID = data[originAirportSeqIDIdx];
					destAirportSeqID = data[destAirportSeqIDIdx];
					originCityMarketID = data[originCityMarketIDIdx];
					destCityMarketID = data[destCityMarketIDIdx];
					originStateFips = data[originStateFipsIdx];
					destStateFips = data[destStateFipsIdx];
					originWac = data[originWacIdx];
					destWac = data[destWacIdx];
					cancelled = data[cancelledIdx];
					origin = data[originIdx];
					destination = data[destinationIdx];
					originCityName = data[originCityNameIdx];
					destCityName = data[destCityNameIdx];
					originStateAbr = data[originStateAbrIdx];
					destStateAbr = data[destStateAbrIdx];
					originStateName = data[originStateNameIdx];
					destStateName = data[destStateNameIdx];
					arrTime = data[arrTimeIdx];
					depTime = data[depTimeIdx];
					actualElapsedTime = data[actualElapsedTimeIdx];
					arrDelay = data[arrDelayIdx];
					arrDelayMinutes = data[arrDelayMinIdx];
					arrDel15 = data[arrDel15Idx];
					monthStr = data[monthIdx];
					yearStr = data[yearIdx];
					if (isNum(monthStr) && StringUtils.isNotBlank(monthStr) && !StringUtils.isEmpty(monthStr)) {
						month = Integer.parseInt(monthStr);
					}
					if (isNum(yearStr) && StringUtils.isNotBlank(yearStr) && !StringUtils.isEmpty(yearStr)) {
						year = yearStr;
					}
					if (!isTimesChecked()) {badData++;continue;}
					if ((timeZone % 60) != 0) {badData++;continue;}
					if (!isIdCorrect()) {badData++;continue;}
					if (!isFieldCorrect()) {badData++;continue;}
					if (!isCancellationStatusCorrect()) {badData++;continue;}
					//if the flights pass every sanity test then they are sane flights.
					goodData++;
					carrier = data[carrIdx];
					price = data[priceIdx];
					if (year.equals("2015"))
						activeIn2015.add(carrier);
					createMap();
				}
				reader.close();
			} catch (FileNotFoundException fe) {
			} catch (IOException ie) {
			}
		}
	}
	/**
	 * @param field -> field to be checked for being numeric
	 * Helper to check if field is numeric*/
	public boolean isNum(String field){
		return StringUtils.isNumeric(field);
	}
	/**
	 * @param field -> field passed to check time is valid
	 * Helper for sanity check of time fields*/
	public boolean timeChecker(String field){
		if(!field.isEmpty()&&StringUtils.isNumeric(field)&&!field.equals("0")&&!field.equals(null)){
			return true;
		}else{
			return false;
		}
	}
	/**
	 * @param field -> id passed for check
	 * Helper for sanity check of ID fields*/
	public boolean idCorrect(String field){
		if( StringUtils.isNotBlank(field)&&field!=null && StringUtils.isNumeric(field))
			return true;
		else
			return false;
	}
	/**
	 * @param field -> take time in hmm:mm format
	 * Helper to convert ArrTime DeptTime and other such fields in format hhmm in minutes**/
	public int convertToMinutes(String field){
		int giventime = 0;
		if(field.length()==3){
			field="0".concat(field);
		}
		if (field.length() > 3) {
			String hrs = field.substring(0, 2);
			String mins = field.substring(2);
			giventime = Integer.parseInt(hrs) * 60 + Integer.parseInt(mins);
		} else {
			String hrs = "0";
			String mins = field;
			giventime = Integer.parseInt(hrs) * 60 + Integer.parseInt(mins);
		}
		return giventime;
	}
	/**
	 * @param field1 -> arrdelay
	 * @param field2 -> arrdelaymins
	 * @param field3 -> arrdelay15
	 * Helper to sanity check of arrdelay arrdelaymins arrdelay15
	 * */
	private static boolean checkArrDelay(String field1, String field2, String field3) {
		double arrdelay=Double.parseDouble(field1);
		double arrdelaymins=Double.parseDouble(field2);
		double arrdelay15=Double.parseDouble(field3);
		if(arrdelay>0 && arrdelay==arrdelaymins)
			return true;
		if(arrdelay<0 && arrdelaymins ==0)
			return true;
		if(arrdelaymins>=15 && arrdelay15==1 )
			return true;
		return false;
	}
	/**
	 * Check CRSArrTime,CRSDepTime,CRSElapsedTime for their validity using timeChecker method .
	 * If they are valid then calculate the time zone.
	 * */
	public boolean isTimesChecked(){
		if(timeChecker(crsArrTime)&&timeChecker(crsDepTime)&&timeChecker(crsElapsedTime)){
			timeZone=convertToMinutes(crsArrTime)-convertToMinutes(crsDepTime)-Integer.parseInt(crsElapsedTime);
			return true;
		}
		return false;
	}
	/**
	 * Check whether the IDs are valid using idCorrect function.
	 * */
	public boolean isIdCorrect(){
		if(idCorrect(originAirportID) && idCorrect(destAirportID) && idCorrect(originAirportSeqID) &&
				idCorrect(destAirportSeqID) && idCorrect(originCityMarketID) && idCorrect(destCityMarketID)
				&& idCorrect(originStateFips) && idCorrect(destStateFips) &&
				idCorrect(originWac) && idCorrect(destWac)) {
			if (Integer.parseInt(originAirportID) < 0 || Integer.parseInt(destAirportID) < 0
					|| Integer.parseInt(originAirportSeqID) < 0 || Integer.parseInt(destAirportSeqID) < 0 ||
					Integer.parseInt(originCityMarketID) < 0 || Integer.parseInt(destCityMarketID) < 0
					|| Integer.parseInt(originStateFips) < 0 || Integer.parseInt(destStateFips) < 0 ||
					Integer.parseInt(originWac) < 0 || Integer.parseInt(destWac) < 0)
				return false;
		} else
			return false;
		return true;
	}
	/**
	 * Sanity test for fields in loop below. Check whether they are empty or not.
	 * */
	public boolean isFieldCorrect(){
		if(origin.equals("") || destination.equals("") || originCityName.equals("") || destCityName.equals("")
				|| originStateAbr.equals("") || destStateAbr.equals("") ||
				originCityName.equals("") || destStateName.equals(""))
			return false;
		return true;
	}
	/**
	 * Sanity test for flights not cancelled.
	 * For flights not cancelled ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
	 * I have checked the condition otherwise. If its true for cancelled flights then it's a bad data.
	 * */
	public boolean isCancellationStatusCorrect(){
		if(cancelled.equals("1")){
			if(isNum(arrTime) && isNum(depTime) && isNum(actualElapsedTime) && isNum(arrDelay)
					&& isNum(arrDelayMinutes) && isNum(arrDel15))
				if((convertToMinutes(arrTime)-convertToMinutes(depTime)-
						Integer.parseInt(actualElapsedTime)-timeZone==0))
					return false;
			if(isNum(arrDelay) && isNum(arrDelayMinutes) && isNum(arrDel15))
				if(!checkArrDelay(arrDelay, arrDelayMinutes, arrDel15))
					return false;
		}
		return true;
	}
	/**
	 * Create a map of carrier and month as key and list of their prices as values.
	 * if map is has a key value pair then add the price for the carrier by retrieving its existing list of
	 * prices and adding the current price.
	 * Add the carrier and updated list in map.
	 * if map is empty or there is a carrier which is not similar to current carrier then create new list add the
	 * price and put it in map.
	 ** */
	public void createMap() {
		String mapkey= month+"\t"+carrier;
		if(map.containsKey(mapkey)){
			List<Double> val=map.get(mapkey);
			val.add(Double.parseDouble(price));
			map.put(mapkey, val);
		}
		else{
			list=new ArrayList<Double>();
			list.add(Double.parseDouble(price));
			map.put(mapkey, list);
		}
	}
}

