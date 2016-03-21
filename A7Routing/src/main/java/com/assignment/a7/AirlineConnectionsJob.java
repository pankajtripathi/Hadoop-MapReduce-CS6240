package com.assignment.a7;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

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

import com.opencsv.CSVParser;

/**
 * @author Shakti Patro
 * @author Pankaj Tripathi
 * @author Kartik Mahaley
 * @author Chen Bai
 * 
 * Functionality :
 * 1. This class reads history file and test file in two different calls.
 * 2. Finds connections and prints them.  
 * 3. Key used is Carrier, Month 
 * 4. Month rollover is handled by sending last day and first Day of month to different reducers
 */
public class AirlineConnectionsJob extends Configured implements Tool {
	static String SEPARATOR = ",";
	static CSVParser csv_parser = new CSVParser();
	static SimpleDateFormat dateFormatter1 = new SimpleDateFormat("MM/dd/yyyy");
	static SimpleDateFormat dateFormatter2 = new SimpleDateFormat("yyyy-MM-dd");
	final static long THIRTY_MINS_MS = 30 * 60000;
	final static long ONE_HR_IN_MS=60*60000;

	/**
	 * @param args: [0]-test/history file [1]- intermediate output
	 * @return job status 
	 *  
	 */
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "AirlineConnectionsJob");
		job.setJarByClass(App.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(ConnectionMapper.class);
		job.setReducerClass(ConnectionReducer.class);
		job.setMapOutputKeyClass(CompositeGroupKey.class);
		job.setMapOutputValueClass(FlightObject.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;

	}

	/**
	 * Mapper input 	: history / test files
	 * Mapper output  
	 * 		key			: Carrier, YearMonth, Origin/Dest Airport
	 * 		value 		: Flight Object with Arriving or Deprating flight details
	 * Uses conf->job.type property to distinguish between train and test data
	 */
	public static class ConnectionMapper extends Mapper<LongWritable, Text, CompositeGroupKey, FlightObject> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (key.get() > 0) {
				String[] flightDetails = csv_parser.parseLine(value.toString());
				String[] finalFlights=new String[110];
				if(flightDetails.length == 111) System.arraycopy(flightDetails, 0, finalFlights, 0, 110);
				else finalFlights = flightDetails;
				if (finalFlights.length == 110) {
					try {
						AirlineDetails airline = null;
						if(context.getConfiguration().get("job.type").equals("train")) {
							airline = new AirlineDetails(finalFlights);
							sanityCheck(airline);
						}
						else airline = new AirlineDetails(finalFlights, true);
						String carrier = airline.getCarrier();
						String yearMonth = airline.getYear() +SEPARATOR+ airline.getMonth();
						String origin = airline.getOrigin();
						String dest = airline.getDestination();
						String flNum = airline.getFlNum();
						short dayOfWeek = airline.getDayOfWeek().shortValue();
						short dayOfMonth = airline.getDayOfMonth().shortValue();
						Calendar flDate = toDateChange(airline.getFlDate());
						short distance=airline.getDistanceGroup().shortValue();
						int crsElapsedTime=airline.getCrsElapsedTime(); 
						long crsArrival = getScheduleTimeInMs(flDate, airline.getCrsArrivalTime());
						long crsDeparture = getScheduleTimeInMs(flDate, airline.getCrsDepartureTime());
						long actArrMinsInMs = getScheduleTimeInMs(flDate, airline.getActualArrivalTime());
						long actDepMinsInMs = getScheduleTimeInMs(flDate, airline.getActualDepartureTime());

						CompositeGroupKey key1 = new CompositeGroupKey(carrier, yearMonth, dest);
						FlightObject f = new FlightObject(true, origin, dest, flNum, crsArrival, actArrMinsInMs, crsDeparture,
								crsElapsedTime, distance, dayOfMonth, dayOfWeek);
						context.write(key1, f);
						if (isLastDayAndInRange(flDate.getTimeInMillis())) {
							flDate.add(Calendar.DATE, 1);
							String yearMonthNew = flDate.get(Calendar.YEAR) + SEPARATOR +flDate.get(Calendar.MONTH);
							CompositeGroupKey key2 = new CompositeGroupKey(carrier, yearMonthNew, dest);
							context.write(key2, f);
						}
						CompositeGroupKey key2 = new CompositeGroupKey(carrier, yearMonth, origin);
						FlightObject g = new FlightObject(false,origin, dest, flNum, crsDeparture, actDepMinsInMs, crsArrival, 
								crsElapsedTime, distance, dayOfMonth, dayOfWeek);
						context.write(key2, g);
						if(isFirstDayAndInRange(flDate.getTimeInMillis())) {
							flDate.add(Calendar.DATE, -1);
							String yearMonthNew = flDate.get(Calendar.YEAR) + SEPARATOR +flDate.get(Calendar.MONTH);
							CompositeGroupKey key3 = new CompositeGroupKey(carrier, yearMonthNew, origin);
							context.write(key3, g);
						}
					} catch (InvalidFormatException |InsaneInputException |  ParseException e) {} 
				}
			}
		}
	}


	/**
	 * Reducer Input 	: (Carrier, YearMonth, Origin/Dest Airport), (List of FlightObjects)
	 * Reducer Output 	: Connections : with missed and duration
	 * Reducer code		: 
	 * 		1. Connections have at least 30 minutes and no more than one hour on the same carrier
	 * 		2. Missed connection predicted if actual time of departing flight is less than 
	 * 		   30 mins that of arrriving flight    
	 */
	public static class ConnectionReducer extends Reducer<CompositeGroupKey, FlightObject, Text, Text> {
		@Override
		public void reduce(CompositeGroupKey key, Iterable<FlightObject> values, Context context)
				throws IOException, InterruptedException {
			List<FlightObject> arrivalFlightObjectList = new LinkedList<FlightObject>();
			List<FlightObject> departureFlightObjectList = new LinkedList<FlightObject>();

			for (FlightObject v : values) {
				FlightObject fo = new FlightObject(v.type, v.origin, v.dest, v.flNum, v.scheduledTime,v.actualTime, 
						v.scheduledTime2, v.crselapsedtime, v.distance, v.dayOfMonth, v.dayOfWeek);
				if(v.type) arrivalFlightObjectList.add(fo);
				else departureFlightObjectList.add(fo);
			}
			Collections.sort(arrivalFlightObjectList);
			Collections.sort(departureFlightObjectList);
			boolean missed=false;
			for (FlightObject f : arrivalFlightObjectList) {
				for (FlightObject g : departureFlightObjectList) {
					if ((g.scheduledTime - f.scheduledTime) > ONE_HR_IN_MS) break;
					if (g.scheduledTime2 - f.scheduledTime2 < 0) continue;
					if ((f.scheduledTime + THIRTY_MINS_MS) < g.scheduledTime) {
						if ((g.actualTime - f.actualTime) < THIRTY_MINS_MS) missed = true;
						long duration = (g.scheduledTime2 - f.scheduledTime2) / ONE_HR_IN_MS;  
						Text value = new Text(f.dayOfMonth +SEPARATOR+ f.dayOfWeek +SEPARATOR+ f.origin +SEPARATOR+ 
								f.dest +SEPARATOR+ g.dest +SEPARATOR+ f.flNum + SEPARATOR + g.flNum +SEPARATOR+ 
								f.scheduledTime + SEPARATOR+ f.crselapsedtime + SEPARATOR + f.distance +SEPARATOR+ 
								duration +SEPARATOR+ missed);
						context.write(new Text(key.name+SEPARATOR+ key.yearMonth), value);
					}
				}
			}
		}
	}


	/**
	 * @param airline
	 * Method for sanity check of airlines.
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
	 * This method takes a time in HHMM format and returns the minute
	 * value as HH*60 + MM Ex: 1030 returns 630.
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
	 * This method takes a date of the flight in MM/DD/YYYY format,
	 * acturaltime of arrival or departure in HHmm and delay in
	 * minutes and returns time of arrival or departure in
	 * milliseconds.
	 */
	private static long getScheduleTimeInMs(Calendar flDate, int CrsTime) throws ParseException {
		long CrsTimeInMs = flDate.getTimeInMillis() + calculateMs(String.valueOf(CrsTime));
		return CrsTimeInMs;
	}

	/**
	 * @param d
	 * Method takes date and parses it to MM/dd/yyyy or yyyy-MM-dd format
	 */
	private static Calendar toDateChange(String d) throws ParseException {
		Date date = null;
		if (d.contains("/")) date = dateFormatter1.parse(d);
		else if (d.contains("-")) date = dateFormatter2.parse(d);
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		return cal;
	}

	/**
	 * @param stime
	 * Method takes string time in hh:mm and converts it into milliseconds
	 */
	private static long calculateMs(String stime) {
		int time = Integer.parseInt(stime);
		int hours = time / 100;
		int minutes = time % 100;
		return (hours * 60 + minutes) * 60000;
	}


	/**
	 * @param timeInMillis
	 * @return if the time is in first day of month and within first 1 hour of the day 
	 */
	public static boolean isFirstDayAndInRange(long timeInMillis) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(timeInMillis);
		return ((calendar.getMinimum(Calendar.DATE) == calendar.get(Calendar.DATE)) 
				&& (calendar.get(Calendar.HOUR_OF_DAY) <= 1));
	}

	/**
	 * @param timeInMillis
	 * @return if the time is in last day of month and within last 1 hour of the day
	 */
	public static boolean isLastDayAndInRange(long timeInMillis) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(timeInMillis);
		return ((calendar.getMaximum(Calendar.DAY_OF_MONTH) == calendar.get(Calendar.DATE))
				&& calendar.get(Calendar.HOUR_OF_DAY) >= 23);
	}


}
