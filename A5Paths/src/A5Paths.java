package com.assignment.A5;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
 * @author Pankaj Tripathi, Kartik Mahaley
 */
public class A5Paths extends Configured implements Tool {
	static String SEPATRATOR = "\t";
	/*
	 * final static int ORIGIN = 0; final static int DEST = 1; final static int
	 * ACT_DEPT = 2; final static int DEPT_DELAY = 3; final static int ACT_ARR =
	 * 4; final static int ARR_DELAY = 5; final static int CANCEL = 6; final
	 * static int FL_DATE = 7;
	 */
	final static long THIRTY_MINS_MS = 30 * 60000;
	final static long SIX_HRS_IN_MS = 360 * 60000;
	

	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();
		File f = new File(args[1]);
		FileUtils.deleteDirectory(f);
		int res = ToolRunner.run(new A5Paths(), args);
		long stop = System.currentTimeMillis();
		System.out.println("Time : " + (float) (stop - start) / 60000);
		System.exit(res);

	}

	public int run(String[] args) throws Exception {
		if (args.length <= 3) {
			System.out.println("usage: [input] [output1] [output2] [garbage]");
			System.exit(-1);
		}
		Configuration configuration = new Configuration();
		Job job = new Job(configuration, "A5Paths");
		job.setJarByClass(A5Paths.class);
		Path inputpath = new Path(args[0]);
		Path outputpath = new Path(args[1]);
		// WholeFileInputFormat.addInputPath(job, inputpath);
		FileInputFormat.addInputPath(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);
		job.setMapperClass(ProjectMapper.class);
		job.setReducerClass(ProjectReducer.class);
		job.setMapOutputKeyClass(CompositeGroupKey.class);
		job.setMapOutputValueClass(flightObject.class);
		job.setOutputKeyClass(CompositeGroupKey.class);
		job.setOutputValueClass(Text.class);

		//return job.waitForCompletion(true) ? 0 : 1;
		job.waitForCompletion(true);
		
		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2, "A5PathsCombineFiles");
		job2.setJarByClass(A5Paths.class);
		Path inputpath2 = new Path(args[1]);
		Path outputpath2 = new Path(args[2]);
		FileInputFormat.addInputPath(job2, inputpath2);
		FileOutputFormat.setOutputPath(job2, outputpath2);
		job2.setMapperClass(mapper2.class);
		job2.setReducerClass(reducer2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		return job2.waitForCompletion(true) ? 0 : 1;
        
		
	}

	/**
	 * Mapper code: Divided each record into two with record's destination and
	 * record's origin details. This was tried to see if code gives some
	 * performance boost
	 */
	public static class ProjectMapper extends Mapper<LongWritable, Text, CompositeGroupKey, flightObject> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Don't Process the header row
			if (key.get() > 0) {
				String[] flightDetails = null;
				// System.out.println("****************" +value);
				String line = parseCityName(value.toString()).replaceAll("\"", "");
				flightDetails = line.split(",");

				if (flightDetails.length == 110) {
					try {
						AirlineDetails airline = new AirlineDetails(flightDetails);
						sanityCheck(airline);
						String carrier = airline.getCarrier();
						String year = airline.getYear().toString();

						String origin = airline.getOrigin();
						String dest = airline.getDestination();
						String fl_date = airline.getFlDate();

						long ScheduleArrival = getScheduleTimeInMs(fl_date, airline.getActualArrivalTime(),
								airline.getArrivalDelay());
						long actArrMinsInMs = calculateMs(String.valueOf(airline.getActualArrivalTime()));

						long ScheduleDeparture = getScheduleTimeInMs(fl_date, airline.getActualDepartureTime(),
								airline.getDepartureDelay());
						long actDepMinsInMs = calculateMs(String.valueOf(airline.getActualDepartureTime()));
						short cancel = (short) airline.getCancelled();

						CompositeGroupKey key1 = new CompositeGroupKey(carrier, year, dest);
						flightObject fo1 = new flightObject(true, ScheduleArrival, actArrMinsInMs, cancel);
						//System.out.println(key1+"  "+fo1.toString());
						context.write(key1, fo1);
						
						CompositeGroupKey key2 = new CompositeGroupKey(carrier, year, origin);
						flightObject fo2 = new flightObject(false, ScheduleDeparture, actDepMinsInMs, cancel);
						//System.out.println(key2+"  "+fo2.toString());
						context.write(key2, fo2);

					} catch (InvalidFormatException e) {
						// e.printStackTrace();
					} catch (InsaneInputException e) {
						// e.printStackTrace();
					} catch (ParseException e) {
						// e.printStackTrace();
					}
				}
			}
		}
	}

	/**
	 * Reducer code: Implements executor service.
	 */
	public static class ProjectReducer extends Reducer<CompositeGroupKey, flightObject, CompositeGroupKey, Text> {
		@Override
		public void reduce(CompositeGroupKey key, Iterable<flightObject> values, Context context)
				throws IOException, InterruptedException {
			List<flightObject> arrivalFlightObjectList = new LinkedList<flightObject>();
			List<flightObject> deptFlightObjectList = new LinkedList<flightObject>();

			for (flightObject v : values) {
				flightObject fo=new flightObject(v.type,v.scheduledTime,v.actualTime,v.cancel);
				if (fo.type)
					arrivalFlightObjectList.add(fo);
				else
					deptFlightObjectList.add(fo);
			}
			Collections.sort(arrivalFlightObjectList);
			Collections.sort(deptFlightObjectList);
			
			int missedFlight = 0, connectionFLight = 0;
			for (flightObject f : arrivalFlightObjectList) {
				
				for (flightObject g : deptFlightObjectList) {
					if ((g.scheduledTime - f.scheduledTime) < SIX_HRS_IN_MS) {
						if ((f.scheduledTime + THIRTY_MINS_MS) < g.scheduledTime
								&& g.scheduledTime < (f.scheduledTime + SIX_HRS_IN_MS)) {
							//System.out.println("*********************** inner inner if");
							connectionFLight++;
							if ((g.actualTime - f.actualTime) < THIRTY_MINS_MS || f.cancel==1)
								missedFlight++;
						}

					} else {
						break;
					}
				}
			}

			String output = connectionFLight + "\t" + missedFlight;
			context.write(key, new Text(output));

		}
	}

	/**
	 * @param row
	 *            below steps are done to replace any "comma" inside the data
	 *            with a "semicolon" code referred from stack overflow
	 */
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
	 * @param airline
	 *            Method for sanity check of airlines.
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
	 * @param time
	 *            This method takes a time in HHMM format and returns the minute
	 *            value as HH*60 + MM Ex: 1030 returns 630.
	 */
	private static int calculateMinutes(Integer time) {
		int hours = time / 100;
		int minutes = time % 100;
		return hours * 60 + minutes;
	}

	/**
	 * @param date
	 * @param actualTime
	 * @param delay
	 *            This method takes a date of the flight in MM/DD/YYYY format,
	 *            acturaltime of arrival or departure in HHmm and delay in
	 *            minutes and returns time of arrival or departure in
	 *            milliseconds.
	 */
	private static long getScheduleTimeInMs(String flDate, int actualTimeOfDay, double delay) throws ParseException {
		Date date = toDateChange(flDate);
		long scheduleTimeInMs = date.getTime() + calculateMs(String.valueOf(actualTimeOfDay))
				- minsToMs(String.valueOf(delay));
		return scheduleTimeInMs;
	}

	/**
	 * @param d
	 *            Method takes date and parses it to MM/dd/yyyy or yyyy-MM-dd
	 *            format
	 */
	private static Date toDateChange(String d) throws ParseException {
		Date date = null;
		if (d.contains("/")) {
			SimpleDateFormat dateFormatter1 = new SimpleDateFormat("MM/dd/yyyy");
			date = dateFormatter1.parse(d);
		} else if (d.contains("-")) {

			SimpleDateFormat dateFormatter2 = new SimpleDateFormat("yyyy-MM-dd");
			date = dateFormatter2.parse(d);
		}
		return date;
	}

	/**
	 * @param stime
	 *            Method takes time(mins) in string and converts it into
	 *            milliseconds
	 */
	private static long minsToMs(String stime) {
		float ftime = Float.parseFloat(stime);
		int time = (int) ftime;
		return time * 60000;
	}

	/**
	 * @param stime
	 *            Method takes string time in hh:mm and converts it into
	 *            milliseconds
	 */
	private static long calculateMs(String stime) {
		int time = Integer.parseInt(stime);
		int hours = time / 100;
		int minutes = time % 100;
		return (hours * 60 + minutes) * 60000;
	}

	/**
	 * Job2 mapper to combine all the *airline and year data of connection and misconnection
	 * */
	public static class mapper2 extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] arr = line.split(SEPATRATOR);
			//System.out.println(arr[0]+" "+arr[1]+" "+arr[2]+" "+arr[3]+" "+arr[4]);
			String list = arr[3] + SEPATRATOR + arr[4];// connections and
													// missedconnections
			String mapkey = arr[0] + SEPATRATOR + arr[1];// carrier and year
			context.write(new Text(mapkey), new Text(list));
		}
	}

	/**
	 * Job2 reducer
	 * */
	public static class reducer2 extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			long connection=0, missedconnection=0;
			for(Text v :values){
				String[] arr=v.toString().split(SEPATRATOR);
				connection=connection +Long.parseLong(arr[0]);
				missedconnection=missedconnection +Long.parseLong(arr[1]);
			}
			context.write(key, new Text(connection+SEPATRATOR+missedconnection));
		}
	}
}
