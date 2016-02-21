package com.cs6240;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AirlineFare extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new AirlineFare(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		JobConf conf = new JobConf(getConf(), AirlineFare.class);
		conf.setJobName("airlinefares");

		// the keys are words (strings)
		conf.setOutputKeyClass(Text.class);
		// the values are counts (ints)
		conf.setOutputValueClass(DoubleWritable.class);

		conf.setMapperClass(MapClass.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		List<String> other_args = new ArrayList<String>();
		for(int i=0; i < args.length; ++i) {
			try {
				if ("-m".equals(args[i])) {
					conf.setNumMapTasks(Integer.parseInt(args[++i]));
				} else if ("-r".equals(args[i])) {
					conf.setNumReduceTasks(Integer.parseInt(args[++i]));
				} else {
					other_args.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " +
						args[i-1]);
				return printUsage();
			}
		}
		// Make sure there are exactly 2 parameters left.
		if (other_args.size() != 2) {
			System.out.println("ERROR: Wrong number of parameters: " +
					other_args.size() + " instead of 2.");
			return printUsage();
		}
		FileInputFormat.setInputPaths(conf, other_args.get(0));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

		JobClient.runJob(conf);
		return 0;
	}

	/**
	 * For each line of input, break the line into words and emit them as
	 * (<b>airline-month</b>, <b>avg price</b>).
	 */
	public static class MapClass extends MapReduceBase
	implements Mapper<LongWritable, Text, Text, DoubleWritable> {
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, DoubleWritable> output,
				Reporter reporter) throws IOException {

			// do not process header row
			if(key.get() > 0) {
				String[] flightDetails = null;
				String line = parseCityName(value.toString()).replaceAll("\"", "");
				flightDetails = line.split(",");

				if(flightDetails.length == 110) {
					try {
						AirlineDetails airline = new AirlineDetails(flightDetails);
						sanityCheck(airline);
						if(airline.getYear() == 2015) {
							word.set(airline.getCarrier() + airline.getMonth());
							output.collect(word, new DoubleWritable(airline.getPrice()));
						}

					} catch (InvalidFormatException e) {
						e.printStackTrace();
					} catch (InsaneInputException e) {
						e.printStackTrace();
					}
				}

			}

		}
	}

	/**
	 * A reducer class that just emits the average, m of the input values.
	 */
	public static class Reduce extends MapReduceBase
	implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		public void reduce(Text key, Iterator<DoubleWritable> values,
				OutputCollector<Text, DoubleWritable> output,
				Reporter reporter) throws IOException {
			double sum = 0, count = 0;
			while (values.hasNext()) {
				sum += values.next().get();
				count++;
			}
			output.collect(key, new DoubleWritable(sum/count));
		}
	}

	static int printUsage() {
		System.out.println("AirlineFare [-m <maps>] [-r <reduces>] <input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}


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


	private static void sanityCheck(AirlineDetails airline) throws InsaneInputException{

		//calculate timezone
		int crsArrTimeInMinutes = calculateMinutes(airline.getCrsArrivalTime());
		int crsDepTimeInMinutes = calculateMinutes(airline.getCrsDepartureTime());
		int crsElapsedTimeInMinutes = airline.getCrsElapsedTime();
		int actualArrTimeInMinutes = calculateMinutes(airline.getActualArrivalTime());
		int actualDepTimeInMinutes = calculateMinutes(airline.getActualDepartureTime());
		int actualElapsedTimeInMinutes = airline.getActualElapsedTime();
		int timezone = crsArrTimeInMinutes - crsDepTimeInMinutes - crsElapsedTimeInMinutes;
		int actulaTimezone = actualArrTimeInMinutes - 	actualDepTimeInMinutes - actualElapsedTimeInMinutes - timezone;

		boolean condition1 = (crsArrTimeInMinutes == 0 && crsDepTimeInMinutes == 0);
		boolean condition2 = (timezone % 60 != 0);
		boolean condition3 = (airline.getOriginAirportId() < 1 || airline.getOriginAirportSequenceId() < 1 
				|| airline.getOriginCityMarketId() < 1 || airline.getOriginStateFips() < 1 
				|| airline.getOriginWac() < 1
				|| airline.getDestinationAirportId() < 1 || airline.getDestinationAirportSequenceId() < 1 
				|| airline.getDestinationCityMarketId() < 1 || airline.getDestinationStateFips() < 1 
				|| airline.getDestinationWac() < 1);
		boolean condition4 = StringUtils.isEmpty(airline.getOrigin()) || StringUtils.isEmpty(airline.getOriginCityName())
				|| StringUtils.isEmpty(airline.getOriginStateName()) || StringUtils.isEmpty(airline.getOriginStateAbbr())
				|| StringUtils.isEmpty(airline.getDestination()) || StringUtils.isEmpty(airline.getDestinationCityName())
				|| StringUtils.isEmpty(airline.getDestinationStateName()) || StringUtils.isEmpty(airline.getDestinationStateAbbr());
		
		boolean condition5 = (airline.getCancelled() == 0) && 
				(actulaTimezone % 24 != 0);
				
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
