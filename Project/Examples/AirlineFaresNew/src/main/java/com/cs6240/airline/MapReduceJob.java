package com.cs6240.airline;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.cs6240.exception.InsaneInputException;
import com.cs6240.exception.InvalidFormatException;
import com.mr.io.InCollector;
import com.mr.io.OutCollector;
import com.mr.io.Tuple;
import com.mr.mapreduce.Mapper;
import com.mr.mapreduce.Reducer;
import com.mr.net.App;

/*
 * author: Shakti Patro 
 */

/*
 * Class: To set configuaration and job properties to run mapreduce job
 */
public class MapReduceJob {

	static Set<String> flightsActiveIn2015 = new HashSet<String>();
	static String operation = "mean";
	
	public static void run() throws Exception {
		App<String,String> w = 
				new App<String,String>( // Initialization
						new MapperClass(), // Mapper
						new ReducerClass() // Reducer
						);
		// Run the workflow; send results to the InCollector
		w.run();	
	}
	
	public static class MapperClass implements Mapper<String,String> {

		@Override
		public void map(OutCollector<String,String> out,
				Tuple<String,String> value) {
			
			if(value.getKey() != "0") {
				String[] flightDetails = null;
				String line = parseCityName(value.getValue()).replaceAll("\"", "");
				flightDetails = line.split(",");
				if(flightDetails.length == 110) {
					try {
						AirlineDetailsPojo airline = new AirlineDetailsPojo(flightDetails);
						AirlineSanity.sanityCheck(airline);
						if(airline.getYear() == 2015) {
							flightsActiveIn2015.add(airline.getCarrier());
						}
						CompositeGroupKey combo = new CompositeGroupKey(airline.getCarrier(), airline.getMonth().toString());
						out.collect(new Tuple<String, String>(combo.toString(), String.valueOf(airline.getPrice())));
					} catch (InvalidFormatException e) {
						e.printStackTrace();
					} catch (InsaneInputException e) {
						e.printStackTrace();
					}
				}
			}
			
		}
	}
	
	public static class ReducerClass implements Reducer<String,String> {

		@Override
		public void reduce(OutCollector<String, String> out, String key,
				InCollector<String, String> values) {
			List<Double> valueList = new ArrayList<Double>();
			while (values.hasNext()) {
				valueList.add(Double.valueOf(values.next().getValue()));
			}
			double calculatedPrice ;
			if(operation.equals("mean")) {
				calculatedPrice = Calculations.calculateMean(valueList);
			} else if(operation.equals("fastmedian")){
				calculatedPrice = Calculations.calculateFastMedian(valueList);
			} else {
				calculatedPrice = Calculations.calculateMedian(valueList);
			}
			out.collect(new Tuple<String,String>(key, String.valueOf(calculatedPrice)));
		}
	}



	/**
	 * 
	 * @param row : the whole row of a csv input 
	 * @return row without a quote in the individual columns.
	 */
	private static String parseCityName(String row) {
		StringBuilder builder = new StringBuilder(row);

		//below steps are done to replace any "comma" inside the data with a "semicolon"
		//code referred from stack overflow
		boolean inQuotes = false;
		for (int currentIndex = 0; currentIndex < builder.length(); currentIndex++) {
			char currentChar = builder.charAt(currentIndex);
			if (currentChar == '\"') inQuotes = !inQuotes; // toggle state
			if (currentChar == ',' && inQuotes) {
				builder.setCharAt(currentIndex, ';'); 
			}
		}
		return builder.toString();
	}

}
