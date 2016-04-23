package com.cs6240.airline;
/*
 * @author : Shakti Patro
 * Class Name : FileEvaluator.java
 * Purpose : i.   Reads one CSV file with all airlines data
 * 			 ii.  Validates data and counts number of valid and invalid rows
 * 			 iii. Returns to the caller, the map with all the airline values and K, F values
 * 
 */	
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.zip.GZIPInputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.cs6240.exception.InsaneInputException;
import com.cs6240.exception.InvalidFormatException;

public class FileEvaluator implements Callable<Map<String, List<Double>>>{

	private final File file;
	private int k = 0, f = 0;
	private Map<String, List<Double>> flightPriceMap = new HashMap<String, List<Double>>();
	static Set<String> flightsActiveIn2015 = new HashSet<String>();
	/*
	 * Constructor class :
	 *  input : File (csv.gz)
	 *  
	 */
	public FileEvaluator(File file) {
		this.file = file;
	}

	public static Set<String> getFlightsActiveIn2015() {
		return flightsActiveIn2015;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.Callable#call()
	 * Call method implementation. Reads the CSV, creates the Map, Returns the map.
	 */
	@Override
	public Map<String, List<Double>> call() {

		this.readCSVAndCreatePriceMapForSaneFlights();
		flightPriceMap.put("K", Arrays.asList(Double.valueOf(k)));
		flightPriceMap.put("F", Arrays.asList(Double.valueOf(f)));
		return this.flightPriceMap;
	}

	/*
	 * Method does follwing tasks
	 * 		1. Reads CSV file and validates it.
	 * 		2. Sets the K and F counts for sane/insane flights
	 * 		3. Creates Map with list of prices for carriers 
	 */
	private void readCSVAndCreatePriceMapForSaneFlights() {
		BufferedReader reader = null;
		CSVParser parser = null;
		try {
			FileInputStream fin = new FileInputStream(file);
			GZIPInputStream gzis = new GZIPInputStream(fin);
			InputStreamReader xover = new InputStreamReader(gzis);
			reader = new BufferedReader(xover);
			parser = new CSVParser(reader, CSVFormat.EXCEL.withHeader());
			for (final CSVRecord record : parser) {
				if(record.size() == 110) {
					try {
						AirlineDetailsPojo airline = new AirlineDetailsPojo(record);
						AirlineSanity.sanityCheck(airline);
						createMapWithFlightDetails(airline);
						if(airline.getYear() == 2015) {
							flightsActiveIn2015.add(airline.getCarrier());
						}
						f++;
					}catch (InsaneInputException e) {
						k++; // input is failing saity test
					}catch (InvalidFormatException e) {
						k++; //input is not valid
					}
				} else {
					k++;// no of columns is not matching the headers 
				}
			}
			k++; //As header is not a valid line
		} catch(FileNotFoundException e){
			System.err.println("File not Found Exception occured");
		} catch (IOException e1) {
			System.err.println("IO Exception occured");
		} finally {
			try {
				parser.close();
				reader.close();
			} catch (IOException e) {
				System.err.println("IOException occured");
			}
		}
	}

	/* 
	 * Create a map with key as flight Carrier and value as a list of prices 
	 */
	private void createMapWithFlightDetails(AirlineDetailsPojo airline) {

		String key = airline.getMonth()+"\t"+airline.getCarrier();
		if(flightPriceMap.get(key) == null) { //inserting list for first time 
			List<Double> priceList = new ArrayList<Double>();
			priceList.add(airline.getPrice());
			flightPriceMap.put(key, priceList);
		}else {
			List<Double> priceList = flightPriceMap.get(key);
			priceList.add(airline.getPrice());
			flightPriceMap.put(key, priceList);
		}
	}

}
