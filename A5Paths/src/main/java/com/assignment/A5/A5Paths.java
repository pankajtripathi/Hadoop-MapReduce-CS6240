package com.assignment.A5;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Pankaj Tripathi, Kartik Mahaley
 * */
public class A5Paths extends Configured implements Tool {

	//CONSTANTS
	static String SEPARATOR = ",";
	final static int ORIGIN = 0;
	final static int DEST = 1;
	final static int ACT_DEPT = 2;
	final static int DEPT_DELAY = 3;
	final static int ACT_ARR = 4;
	final static int ARR_DELAY = 5;
	final static int CANCEL = 6;
	final static int FL_DATE = 7;
	final static long THIRTY_MINS_MS = 30*60000;
	final static long SIX_HRS_IN_MS = 360*60000;
	final static int HUNDRED = 100;
	final static int SIXTY_THOUSAND = 60000;
	final static int SIXTY = 60;
	final static int TWO = 2;
	final static int THOUSAND = 1000;
	static int missedFlight = 0, connectionFLight = 0;

	public static void main(String[] args) throws Exception {
		File f = new File(args[1]);
		FileUtils.deleteDirectory(f);
		long start = System.currentTimeMillis()/THOUSAND;
		int res = ToolRunner.run(new A5Paths(), args);
		long end = System.currentTimeMillis()/THOUSAND;
		System.out.println(end-start);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		if (args.length != TWO) {
			System.out.println("usage: [input] [output]");
			System.exit(-1);
		}
		Configuration configuration = new Configuration();
		Job job = new Job(configuration, "A5Paths");
		job.setJarByClass(A5Paths.class);
		Path inputpath = new Path(args[0]);
		Path outputpath = new Path(args[1]);
		FileInputFormat.addInputPath(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);
		job.setPartitionerClass(NaturalKeyPartitioner.class);
		job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);
		job.setMapOutputKeyClass(CompositeGroupKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(ProjectMapper.class);
		job.setReducerClass(ProjectReducer.class);
		job.setOutputKeyClass(CompositeGroupKey.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Mapper code
	 * */
	public static class ProjectMapper extends Mapper<LongWritable, Text, CompositeGroupKey, Text> {
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
						String carrier = airline.getCarrier();
						String year = airline.getYear().toString();
						String emitDestinationData = airline.getOrigin() + SEPARATOR 
								+ airline.getDestination() + SEPARATOR
								+ airline.getActualDepartureTime() + SEPARATOR
								+ airline.getDepartureDelay() + SEPARATOR 
								+ airline.getActualArrivalTime() + SEPARATOR
								+ airline.getArrivalDelay() + SEPARATOR 
								+ airline.getCancelled() + SEPARATOR
								+ airline.getFlDate();
						CompositeGroupKey compositekey = new CompositeGroupKey(carrier, year);
						context.write(compositekey, new Text(emitDestinationData));
					} catch (InvalidFormatException e) {
						// e.printStackTrace();
					} catch (InsaneInputException e) {
						// e.printStackTrace();
					}
				}
			}
		}
	}

	/**
	 * Reducer code
	 * */
	public static class ProjectReducer extends Reducer<CompositeGroupKey, Text, CompositeGroupKey, Text> {
		@Override
		public void reduce(CompositeGroupKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String[]> itrList = new ArrayList<String []>();
			for(Text v:values)
				itrList.add(v.toString().split(SEPARATOR));
			checkMissedConnections(itrList);
			String out = connectionFLight + "\t" + missedFlight;
			context.write(key, new Text(out));
		}
	}

	/**
	 * @param values
	 * Method checks the missed connections condition mentioned in the the problem statement.
	 * A connection is any pair of flight F and G of the same carrier such as F.Destination = G.Origin and 
	 * the scheduled departure of G is  <= 6 hours and >= 30 minutes after the scheduled arrival of F.
	 * A connection is missed when the actual arrival of F < 30 minutes before the actual departure of G.
	 * */
	private static void checkMissedConnections(ArrayList<String[]> values){
		connectionFLight=0;missedFlight=0;
		for (String[] v1 : values) {
			String[] f = v1;
			String f_fldate = f[FL_DATE];
			long f_time = calculateMs(f[ACT_ARR]) - minsToMs(f[ARR_DELAY]);
			for (String[] v2 : values) {
				String[] g = v2;
				String g_fldate=g[FL_DATE];
				long g_time = calculateMs(g[ACT_DEPT]) - minsToMs(g[DEPT_DELAY]); 
				try {
					if (f[DEST].equals(g[ORIGIN])&&compareDepCond(f_fldate, f_time, g_fldate, g_time)){
						connectionFLight++;
						if (f[CANCEL].equals("1") || flightMissed(f[ACT_ARR],g[ACT_DEPT]))
							missedFlight++;
					}
				} catch (ParseException e) {}
			}
		}
	}

	/**
	 * @param act_arr
	 * @param act_dept
	 * Method checks the condition for flight missed
	 * A connection is missed when the actual arrival of F < 30 minutes before the actual departure of G.
	 * */
	private static boolean flightMissed(String act_arr, String act_dept){
		if((calculateMs(act_dept) - calculateMs(act_arr)) < THIRTY_MINS_MS) return true;
		else return false;
	}

	/**
	 * @param f_fldate
	 * @param f_time
	 * @param g_fldate
	 * @param g_time
	 * Method takes the arrival and departure date and time and converts them to milliseconds.
	 * Then it checks the condition for the difference required for connection
	 * */
	private static boolean compareDepCond(String f_fldate,long f_time,String g_fldate, long g_time) 
			throws ParseException{
		Date f_date=toDateChange(f_fldate);
		Date g_date=toDateChange(g_fldate);
		long f_date_inms = f_date.getTime()  +  f_time;
		long g_date_inms = g_date.getTime() + g_time;
		if((f_date_inms+THIRTY_MINS_MS)<g_date_inms && g_date_inms<(f_date_inms+SIX_HRS_IN_MS))
			return true;
		else
			return false;
	}

	/**
	 * @param d
	 * Method takes date and parses it to MM/dd/yyyy or yyyy-MM-dd format
	 * */
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
	 * @param row 
	 * below steps are done to replace any "comma" inside the data with a
	 * "semicolon" code referred from stack overflow
	 * */
	private static String parseCityName(String row) {
		StringBuilder builder = new StringBuilder(row);
		boolean inQuotes = false;
		for (int currentIndex = 0; currentIndex < builder.length(); currentIndex++) {
			char currentChar = builder.charAt(currentIndex);
			if (currentChar == '\"')
				inQuotes = !inQuotes;
			if (currentChar == ',' && inQuotes) {
				builder.setCharAt(currentIndex, ';');
			}
		}
		return builder.toString();
	}

	/**
	 * @param airline
	 * Method for sanity check of airlines.
	 * */
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
	 * This method takes a time in HHMM format and returns the minute value as
	 * HH*60 + MM Ex: 1030 returns 630.
	 * */
	private static int calculateMinutes(Integer time) {
		int hours = time / HUNDRED;
		int minutes = time % HUNDRED;
		return hours * SIXTY + minutes;
	}

	/**
	 * @param stime
	 * Method takes time(mins) in string and converts it into milliseconds
	 * */
	private static long minsToMs(String stime){
		float ftime = Float.parseFloat(stime);
		int time = (int) ftime;
		return time * SIXTY_THOUSAND;
	}

	/**
	 * @param stime
	 * Method takes string time in hh:mm and converts it into milliseconds
	 * */
	private static long calculateMs(String stime) {
		int time = Integer.parseInt(stime);
		int hours = time / HUNDRED;
		int minutes = time % HUNDRED;
		return (hours * SIXTY + minutes) * SIXTY_THOUSAND;
	}

	/**
	 * The method designed with reference from 
	 * https://vangjee.wordpress.com/2012/03/20/secondary-sorting-aka-sorting-values-in-hadoops-mapreduce-programming-paradigm/
	 * The composite key comparator is where the secondary sorting takes place. 
	 * It compares composite key by name ascending order and year descending order. 
	 * */
	public static class CompositeKeyComparator extends WritableComparator {
		protected CompositeKeyComparator() {
			super(CompositeGroupKey.class, true);
		}   
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			CompositeGroupKey k1 = (CompositeGroupKey)w1;
			CompositeGroupKey k2 = (CompositeGroupKey)w2;
			int result = k1.name.compareTo(k2.name);
			if(0 == result) {result = -1* k1.year.compareTo(k2.year);}
			return result;
		}
	}

	/**
	 * The method designed with reference from 
	 * https://vangjee.wordpress.com/2012/03/20/secondary-sorting-aka-sorting-values-in-hadoops-mapreduce-programming-paradigm/ 
	 * The natural key group comparator “groups” values together according to the natural key. 
	 * Without this component, each K2={name,year} and its associated value may go to different reducers.
	 * */
	public static class NaturalKeyGroupingComparator extends WritableComparator {
		protected NaturalKeyGroupingComparator() {
			super(CompositeGroupKey.class, true);
		}   
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			CompositeGroupKey k1 = (CompositeGroupKey)w1;
			CompositeGroupKey k2 = (CompositeGroupKey)w2;
			int res = k1.name.compareTo(k2.name);
			if(0!=res){	return res;}
			else res=k1.year.compareTo(k2.year);
			return res;
		}
	}

	/**
	 * The method designed with reference from 
	 * https://vangjee.wordpress.com/2012/03/20/secondary-sorting-aka-sorting-values-in-hadoops-mapreduce-programming-paradigm/ 
	 * The natural key partitioner uses the natural key to partition the data to the reducer(s). 
	 * In this case we consider only the natural key.
	 * */
	public static class NaturalKeyPartitioner extends Partitioner<CompositeGroupKey, Text> {
		@Override
		public int getPartition(CompositeGroupKey key, Text val, int numPartitions) {
			int hash = key.name.hashCode()*key.year.hashCode()*277;
			int partition = hash % numPartitions;
			return partition;
		}
	}
}
