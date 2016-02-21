package com.assignment.A4;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/*
 * @author : Pankaj Tripathi, Kartik Mahaley
 * Class Name : A4Regression.java
 */
public class A4Regression extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = new Job(configuration, "A4Regression");
		job.setJarByClass(A4Regression.class);

		Path inputpath = new Path(args[1]);
		Path outputpath = new Path(args[2]);
		FileInputFormat.addInputPath(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);
		job.setMapperClass(ProjectMapper.class);
		job.setReducerClass(ProjectReducer.class);
		job.setOutputKeyClass(CompositeGroupKey.class);
		job.setOutputValueClass(DoubleWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class ProjectMapper extends Mapper<LongWritable, Text, CompositeGroupKey, DoubleWritable> {
		/*
		 * @author : Kartik Mahaley, Pankaj Tripathi 
		 * Function Name : map 
		 * Purpose : Based on year carrier and elapsed gives price.
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// do not process header row
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
						double price = airline.getPrice();
						CompositeGroupKey compositekey = new CompositeGroupKey(aircode, year, elapsedtime);
						context.write(compositekey, new DoubleWritable(price));
					} catch (InvalidFormatException e) {
						e.printStackTrace();
					} catch (InsaneInputException e) {
						e.printStackTrace();
					}
				}

			}

		}
	}

	public static class ProjectReducer
			extends Reducer<CompositeGroupKey, DoubleWritable, CompositeGroupKey, DoubleWritable> {
		/*
		 * @author : Kartik Mahaley, Pankaj Tripathi 
		 * Function Name : reduce 
		 * Purpose : Based on year carrier and elapsed time gave mean price.
		 */
		@Override
		public void reduce(CompositeGroupKey key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			List<Double> pricecache = new ArrayList<Double>();
			// List<Integer> timecache = new ArrayList<Integer>();
			for (DoubleWritable v : values) {
				pricecache.add(v.get());
			}
			double meanvalue = getMean(pricecache);
			context.write(key, new DoubleWritable(meanvalue));
		}

	}

	/*
	 * @author : Kartik Mahaley, Pankaj Tripathi 
	 * Function Name : getMean 
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

	private static String parseCityName(String row) {
		StringBuilder builder = new StringBuilder(row);

		// below steps are done to replace any "comma" inside the data with a
		// "semicolon"
		// code referred from stack overflow
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

	/*
	 * @author : Kartik Mahaley, Pankaj Tripathi 
	 * Function Name : sanityCheck 
	 * Purpose : This function checks if the given record is sane or not
	 */
	private static void sanityCheck(AirlineDetails airline) throws InsaneInputException {

		// calculate timezone
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

	/*
	 * This method takes a time in HHMM format and returns the minute value as
	 * HH*60 + MM Ex: 1030 returns 630.
	 */
	private static int calculateMinutes(Integer time) {
		int hours = time / 100;
		int minutes = time % 100;
		return hours * 60 + minutes;
	}

}
