package com.mapreduce.assignment;

import java.io.BufferedWriter;
/*code for quick sort is taken from github of 
 * djitz*/
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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

/*
 * @author : Kartik Mahaley, Pankaj Tripathi
 * Class Name : AirlineFare.java
 * Purpose : Runs the toolrunner with driver.
 * Examples : produces output as m C v (mean carrier value). 
 * 
 */
public class AirlineFare extends Configured implements Tool {
	static public String getmeanormedian = "mean";

	/*
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new AirlineFare(), args);
		System.exit(res);
	}

	 */
	public int run(String args[]) throws Exception {

		JobConf conf = new JobConf(getConf(), AirlineFare.class);
		conf.setJobName("airlinefares");
		conf.setMapOutputKeyClass(CompositeGroupKey.class);
		conf.setMapOutputValueClass(DoubleWritable.class);
		conf.setOutputKeyClass(CompositeGroupKey.class);
		conf.setOutputValueClass(DoubleWritable.class);
		conf.setMapperClass(MapClass.class);

		conf.setReducerClass(Reduce.class);
		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
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
				System.out.println("ERROR: Required parameter missing from " + args[i - 1]);
				return printUsage();
			}
		}
		// Make sure there are exactly 3 parameters left.
		if (other_args.size() != 3) {
			System.out.println("ERROR: Wrong number of parameters: " + other_args.size() + " instead of 2.");
			return printUsage();
		}
		FileInputFormat.setInputPaths(conf, other_args.get(0));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));
		getmeanormedian = other_args.get(2);

		JobClient.runJob(conf);
		return 0;
	}

	/**
	 * For each line of input, break the line into words and emit them as (
	 * <b>Composite key, month airlinecode</b>, <b>price</b>).
	 */
	public static class MapClass extends MapReduceBase
	implements Mapper<LongWritable, Text, CompositeGroupKey, DoubleWritable> {

		public void map(LongWritable key, Text value, OutputCollector<CompositeGroupKey, DoubleWritable> output,
				Reporter reporter) throws IOException {
			// do not process header row
			if (key.get() > 0) {
				String[] flightDetails = null;
				String line = parseCityName(value.toString()).replaceAll("\"", "");
				flightDetails = line.split(",");

				if (flightDetails.length == 110) {
					try {
						AirlineDetails airline = new AirlineDetails(flightDetails);
						sanityCheck(airline);
						// Populate airline active in 2015 year
						String aircode = airline.getCarrier();
						String month = airline.getMonth().toString();
						CompositeGroupKey compo = new CompositeGroupKey(aircode, month);
						output.collect(compo, new DoubleWritable(airline.getPrice()));

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
	 * A reducer class that just emits the mean or median value of price for a
	 * given composite key of month and carrier.
	 */
	public static class Reduce extends MapReduceBase
	implements Reducer<CompositeGroupKey, DoubleWritable, CompositeGroupKey, DoubleWritable> {

		public void reduce(CompositeGroupKey key, Iterator<DoubleWritable> values,
				OutputCollector<CompositeGroupKey, DoubleWritable> output, Reporter reporter) throws IOException {
			List<Double> cache = new ArrayList<Double>();
			while (values.hasNext()) {
				double value = values.next().get();
				cache.add(value);
			}
			Double medianvalue = getMedian(cache);
			Double meanvalue = getMean(cache);
			Double fastmedian = getfastmedian(cache);
			if (getmeanormedian.equals("fastmedian")) {
				output.collect(key, new DoubleWritable(fastmedian));
			} else if (getmeanormedian.equals("median")) {
				output.collect(key, new DoubleWritable(medianvalue));
			} else {
				output.collect(key, new DoubleWritable(meanvalue));
			}

		}
	}

	/*
	 * @author : Kartik Mahaley, Pankaj Tripathi Function Name : getMedian
	 * Purpose : For a list of price value it returns median.
	 */
	static Double getMedian(List<Double> values) {
		Collections.sort(values);
		Double median;
		if (values.size() % 2 == 0)
			median = (values.get(values.size() / 2) + values.get(values.size() / 2 - 1)) / 2;
		else
			median = values.get(values.size() / 2);
		return median;
	}

	/*
	 * @author : Kartik Mahaley, Pankaj Tripathi Function Name : getMean Purpose
	 * : For a list of price value it returns mean.
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
	 * This algorithm is studied from
	 * http://blog.teamleadnet.com/2012/07/quick-select-algorithm-find-kth-
	 * element.html The code has been reused from this site.
	 * 
	 * @param values
	 */
	public static double getfastmedian(List<Double> values) {
		int k=values.size()/2;
		if (values == null || values.size() <= k)
			throw new Error();
		int from = 0, to = values.size() - 1;
		// if from == to we reached the kth element
		while (from < to) {
			int r = from, w = to;
			double mid = values.get((r + w) / 2);
			// stop if the reader and writer meets
			while (r < w) {
				if (values.get(r) >= mid) { // put the large values at the end
					double tmp = values.get(w);
					values.set(w, values.get(r));
					values.set(r, tmp);
					w--;
				} else { // the value is smaller than the pivot, skip
					r++;
				}
			}
			// if we stepped up (r++) we need to step one down
			if (values.get(r) > mid)
				r--;
			// the r pointer is on the end of the first k elements
			if (k <= r) {
				to = r;
			} else {
				from = r + 1;
			}
		}
		return values.get(k);
	}

	/**
	 * This method sort the input ArrayList using quick sort algorithm.
	 * 
	 * @param input
	 *            the ArrayList of integers.
	 * @return sorted ArrayList of integers.
	 */

	/*
	 * @author : Kartik Mahaley, Pankaj Tripathi Purpose : Tells the user how to
	 * give parameter to the function.
	 */
	static int printUsage() {
		System.out.println("AirlineFare [-m <maps>] [-r <reduces>] <input> <output> <mean/median>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
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
