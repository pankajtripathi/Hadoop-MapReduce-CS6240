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

/**
 * @author : Pankaj Tripathi, Kartik Mahaley
 * Class Name : GraphPlotter.java
 */
public class GraphPlotter extends Configured implements Tool {

	public int startPlot(String args[]) throws Exception {
		return ToolRunner.run(new GraphPlotter(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.set("carrier", args[2]);
		Job job = new Job(configuration, "GraphPloptter");
		job.setJarByClass(GraphPlotter.class);

		Path inputpath = new Path(args[0]);
		Path outputpath = new Path(args[1]);
		FileInputFormat.addInputPath(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);
		job.setMapperClass(GraphPlotterMapper.class);
		job.setReducerClass(GraphPlotterReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Purpose : returns key as year and week of the year
	 */
	public static class GraphPlotterMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (key.get() > 0) {
				String[] flightDetails = null;
				String line = parseCityName(value.toString()).replaceAll("\"", "");
				flightDetails = line.split(",");
				if (flightDetails.length == 110) {
					try{
						AirlineDetails airline = new AirlineDetails(flightDetails);
						sanityCheck(airline);
						String query = context.getConfiguration().get("carrier");
						if (airline.getCarrier().equals(query)) {
							String year = airline.getYear().toString();
							String week = getWeekOfDate(airline.getFlDate());
							double price = airline.getPrice();
							context.write(new Text(year + "\t" + week), new DoubleWritable(price));
						}
					} catch (InvalidFormatException | InsaneInputException | ParseException e) {
						e.printStackTrace();
					} 
				}
			}
		}
	}

	/**
	 * Purpose : for a given key, takes input as list of the prices and returns median for it
	 */
	public static class GraphPlotterReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			List<Double> pricecache = new ArrayList<Double>();
			for (DoubleWritable v : values) {
				pricecache.add(v.get());
			}
			double medianValue = getMedian(pricecache);
			context.write(key, new DoubleWritable(medianValue));
		}

	}

	/**
	 * Purpose : takes date as string and returns week of the year
	 */
	static String getWeekOfDate(String flDate) throws ParseException {
		Date date = null;
		if (flDate.contains("/")) {
			SimpleDateFormat dateFormatter1 = new SimpleDateFormat("MM/dd/yyyy");
			date = dateFormatter1.parse(flDate);
		} else if (flDate.contains("-")) {
			SimpleDateFormat dateFormatter2 = new SimpleDateFormat("yyyy-MM-dd");
			date = dateFormatter2.parse(flDate);
		}
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		int week = cal.get(Calendar.WEEK_OF_YEAR);
		return String.valueOf(week);
	}

	/**
	 * Purpose : For a list of price value it returns median.
	 */
	static Double getMedian(List<Double> values) {
		Collections.sort(values);
		Double median;
		if (values.size() % 2 == 0)
			median = (values.get(values.size() / 2) + values.get(values.size() / 2 - 1)) / 2;
		else median = values.get(values.size() / 2);
		return median;
	}

	/**
	 * This replaces any "comma" inside the data with a "semicolon"
	 * code referred from stack overflow
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
