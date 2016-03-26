package com.assignment.A4;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.math.stat.regression.SimpleRegression;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author : Pankaj Tripathi, Kartik Mahaley
 * Class Name : A4Regression.java
 */
public class A4Regression{

	private static Map<Integer, CarrierPrice> cheapeastCarrierMap = new HashMap<Integer, CarrierPrice>();
	private static final String CHEAPEAST_CARRIER = "Cheapest";

	public String runRegression(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.set("N", args[2]);
		Job job = new Job(configuration, "A4Regression");
		job.setJarByClass(A4Regression.class);

		Path inputpath = new Path(args[0]);
		Path outputpath = new Path(args[1]);
		FileInputFormat.addInputPath(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);
		job.setMapperClass(ProjectMapper.class);
		job.setReducerClass(ProjectReducer.class);
		job.setOutputKeyClass(CompositeGroupKey.class);
		job.setOutputValueClass(PriceTime.class);

		job.waitForCompletion(true);

		String cheapeastCarrier=null;
		Counters counters = job.getCounters();
		CounterGroup cGroup =  counters.getGroup(CHEAPEAST_CARRIER);
		for(Counter c : cGroup) {
			if(c.getDisplayName().startsWith(CHEAPEAST_CARRIER)) {
				cheapeastCarrier = c.getDisplayName().substring(c.getDisplayName().indexOf("=") + 1); 
			}
		}
		return cheapeastCarrier;
	}

	/**
	 * Purpose : Based on year carrier and elapsed gives price.
	 */
	public static class ProjectMapper extends Mapper<LongWritable, Text, CompositeGroupKey, PriceTime> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (key.get() > 0) {
				String[] flightDetails = null;
				String line = parseCityName(value.toString()).replaceAll("\"", "");
				flightDetails = line.split(",");
				if (flightDetails.length == 110) {
					try {
						AirlineDetails airline = new AirlineDetails(flightDetails);
						sanityCheck(airline);
						String aircode = airline.getCarrier();
						String year = airline.getYear().toString();
						String elapsedtime = String.valueOf(airline.getActualElapsedTime());
						String price = String.valueOf(airline.getPrice());
						PriceTime pricetime=new PriceTime(price,elapsedtime);
						CompositeGroupKey compositekey = new CompositeGroupKey(aircode, year);
						context.write(compositekey, pricetime);
					} catch (InvalidFormatException | InsaneInputException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	/** 
	 * Purpose : Based on year carrier and elapsed time gave mean price.
	 */
	public static class ProjectReducer
	extends Reducer<CompositeGroupKey, PriceTime, Text, Text> {
		@Override
		public void reduce(CompositeGroupKey key, Iterable<PriceTime> values, Context context)
				throws IOException, InterruptedException {
			SimpleRegression regression =new SimpleRegression();
			for (PriceTime v : values) 
				regression.addData(v.getPrice().get(),v.getTime().get());

			double N = Double.parseDouble(context.getConfiguration().get("N"));
			//Compute the estimated Price for the given value of time argument passed through command line
			double estimatedPrice = Math.round(regression.predict(N)*100)/100;
			manipulateCarrierMap(key,estimatedPrice);

		}

		/**
		 * This method adds a CarrierPrice object as a value and Year as the key to a map
		 * If the map does not contain the key, add a new CarrierPrice object with carrier and the estimated price fields
		 * If the map does contain the key, check if the existing CarrierPrice object for that Year key has estimated price
		 * field higher than the current estimated price, if yes, change the CarrierPrice object by setting the
		 * current carrier and current estimated price
		 * @param key
		 * @param estimatedPrice
		 */
		private void manipulateCarrierMap(CompositeGroupKey key, double estimatedPrice) {
			if(cheapeastCarrierMap.containsKey(key.year))
				updateMap(key, estimatedPrice);
			else
				cheapeastCarrierMap.put(Integer.parseInt(key.year), new CarrierPrice(key.name , estimatedPrice));
		}

		/**
		 * This method checks if the existing CarrierPrice object for that Year key has estimated price
		 * field higher than the current estimated price, if yes, change the CarrierPrice object by setting the
		 * current carrier and current estimated price
		 * @param key
		 * @param estimatedPrice
		 */
		private void updateMap(CompositeGroupKey key, double estimatedPrice) {
			if(cheapeastCarrierMap.get(key.year).getPrice() > estimatedPrice) {
				CarrierPrice cep = cheapeastCarrierMap.get(key.year);
				cep.setCarrier(key.name);
				cep.setPrice(estimatedPrice);
			}			
		}

		/**
		 * This method creates a map, with carrier as the key and value as the number of times that carrier was
		 * cheapest in the year 
		 * @param winnerCountMap
		 * @param cheapeastCarrierMap
		 */
		protected void cleanup(Context context) throws IOException, InterruptedException {		
			Map<String, Integer> winnerCountMap = new HashMap<String, Integer>();
			//Create the map which contains key as the carrier and value as the number of times the carrier was cheapest for a year
			createWinnerCountMap(winnerCountMap, cheapeastCarrierMap);
			//Get the cheapest carrier from the map which has the highest value
			String cheapeastCarrier = getCheapeastCarrier(winnerCountMap);		
			context.getCounter(CHEAPEAST_CARRIER, CHEAPEAST_CARRIER + "=" + cheapeastCarrier).increment(1);	
			context.write(new Text(cheapeastCarrier), new Text());
		}

		/**
		 * This method creates a map, with carrier as the key and value as the number of times that carrier was
		 * cheapest in the year 
		 * @param winnerCountMap
		 * @param cheapeastCarrierMap
		 */
		private void createWinnerCountMap(Map<String, Integer> winnerCountMap, Map<Integer, CarrierPrice> cheapeastCarrierMap) {
			for(Map.Entry<Integer, CarrierPrice> entry : cheapeastCarrierMap.entrySet()) {
				if(winnerCountMap.containsKey(entry.getValue().getCarrier()))
					winnerCountMap.put(entry.getValue().getCarrier(), winnerCountMap.get(entry.getValue().getCarrier()) + 1);
				else
					winnerCountMap.put(entry.getValue().getCarrier(), 1);				
			}
		}

		/**
		 * This method finds the cheapest carrier from the winnerCountMap
		 * @param winnerCountMap
		 * @return
		 */
		private String getCheapeastCarrier(Map<String, Integer> winnerCountMap) {
			TreeMap<Integer, String> sortedWinnerMap = new TreeMap<Integer, String>(Collections.reverseOrder());
			for(Map.Entry<String, Integer> entry : winnerCountMap.entrySet())
				sortedWinnerMap.put(entry.getValue(), entry.getKey());
			return sortedWinnerMap.firstEntry().getValue();
		}
	}

	/** 
	 * Purpose : For a list of price value it returns mean.
	 */
	static Double getMean(List<Double> values) {
		Double sum = 0.0, mean = 0.0;
		Integer count = 0;
		for (double v : values) {
			sum += v;
			count++;
		}
		mean = sum / count;
		return mean;
	}

	/**
	 * @param row 
	 * Checks for city name with quotes and other special characters
	 * returns the city name parsed from the line.
	 * */
	private static String parseCityName(String row) {
		StringBuilder builder = new StringBuilder(row);
		boolean inQuotes = false;
		for (int currentIndex = 0; currentIndex < builder.length(); currentIndex++) {
			char currentChar = builder.charAt(currentIndex);
			if (currentChar == '\"')
				inQuotes = !inQuotes; // toggle state
			if (currentChar == ',' && inQuotes) {
				builder.setCharAt(currentIndex, ';');
			}
		}
		return builder.toString();
	}

	/**
	 * Purpose : This function checks if the given record is sane or not
	 */
	private static void sanityCheck(AirlineDetails airline) throws InsaneInputException {
		int crsArrTimeInMinutes = calculateMinutes(airline.getCrsArrivalTime());
		int crsDepTimeInMinutes = calculateMinutes(airline.getCrsDepartureTime());
		int crsElapsedTimeInMinutes = airline.getCrsElapsedTime();
		int actualArrTimeInMinutes = calculateMinutes(airline.getActualArrivalTime());
		int actualDepTimeInMinutes = calculateMinutes(airline.getActualDepartureTime());
		int actualElapsedTimeInMinutes = airline.getActualElapsedTime();
		int timezone = crsArrTimeInMinutes - crsDepTimeInMinutes - crsElapsedTimeInMinutes;
		int actulaTimezone = actualArrTimeInMinutes - actualDepTimeInMinutes - actualElapsedTimeInMinutes - timezone;
		boolean condition1 = (crsArrTimeInMinutes == 0 && crsDepTimeInMinutes == 0);
		boolean condition2 = (timezone % 60 != 0);
		boolean condition3 = (airline.getOriginAirportId() < 1 || airline.getOriginAirportSequenceId() < 1
				|| airline.getOriginCityMarketId() < 1 || airline.getOriginStateFips() < 1 || airline.getOriginWac() < 1
				|| airline.getDestinationAirportId() < 1 || airline.getDestinationAirportSequenceId() < 1
				|| airline.getDestinationCityMarketId() < 1 || airline.getDestinationStateFips() < 1
				|| airline.getDestinationWac() < 1);
		boolean condition4 = StringUtils.isEmpty(airline.getOrigin())
				|| StringUtils.isEmpty(airline.getOriginCityName()) || StringUtils.isEmpty(airline.getOriginStateName())
				|| StringUtils.isEmpty(airline.getOriginStateAbbr()) || StringUtils.isEmpty(airline.getDestination())
				|| StringUtils.isEmpty(airline.getDestinationCityName())
				|| StringUtils.isEmpty(airline.getDestinationStateName())
				|| StringUtils.isEmpty(airline.getDestinationStateAbbr());
		boolean condition5 = (airline.getCancelled() == 0) && (actulaTimezone % 24 != 0);
		boolean condition6 = (airline.getArrivalDelay() > 0)
				&& (airline.getArrivalDelay() != airline.getArrivalDelayMinutes());
		boolean condition7 = (airline.getArrivalDelay() < 0) && (airline.getArrivalDelayMinutes() != 0);
		boolean condition8 = (airline.getArrivalDelayMinutes() > 15) && (airline.getArrivalDelay15() == 0);
		if (condition1 && condition2 && condition3 && condition4 && condition5 && condition6 && condition7
				&& condition8)
			throw new InsaneInputException("Sanity test failed");
	}

	/**
	 * This method takes a time in HHMM format and returns the minute value as
	 * HH*60 + MM Ex: 1030 returns 630.
	 */
	private static int calculateMinutes(Integer time) {
		int hours = time / 100;
		int minutes = time % 100;
		return hours * 60 + minutes;
	}
}
