package com.mapreduce.prediction;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import com.mapreduce.prediction.CompositeGroupKey;
import com.opencsv.CSVParser;

/**
 * @author Pankaj Tripathi, Kartik Mahaley
 * @collaborator Shakti Patro
 * */
public class A6Prediction extends Configured implements Tool {

	//CONSTANTS
	static String SEPARATOR = "\t";
	final static int HUNDRED = 100;
	final static int SIXTY = 60;
	final static int FOUR = 4;
	static int missedFlight = 0, connectionFLight = 0;
	final static SimpleDateFormat form = new SimpleDateFormat("yyyy-MM-dd");

	public int run(String[] args) throws Exception {
		if (args.length != FOUR) {
			System.out.println("usage: [input_history] [input_test] [input_validate] [output]");
			System.exit(-1);
		}
		Configuration configuration = new Configuration();
		Job job = new Job(configuration, "A6Prediction");
		job.setJobName("A6PredictionJob1");
		job.setJarByClass(A6Prediction.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]+"/train"));
		MultipleOutputs.addNamedOutput(job, "a6model", TextOutputFormat.class, CompositeGroupKey.class, Text.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		job.setMapOutputKeyClass(CompositeGroupKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(ProjectMapper.class);
		job.setReducerClass(ProjectReducer.class);
		job.setOutputKeyClass(CompositeGroupKey.class);
		job.setOutputValueClass(Text.class);
		job.submit();

		Job job2 = new Job(configuration, "A6Prediction");
		job2.setJobName("A6PredictionJob2");
		job2.setJarByClass(A6Prediction.class);
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[3]+"/test"));
		MultipleOutputs.addNamedOutput(job2, "a6model", TextOutputFormat.class, CompositeGroupKey.class, Text.class);
		LazyOutputFormat.setOutputFormatClass(job2, TextOutputFormat.class);
		job2.setMapOutputKeyClass(CompositeGroupKey.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setMapperClass(ProjectMapper.class);
		job2.setReducerClass(ProjectReducer.class);
		job2.setOutputKeyClass(CompositeGroupKey.class);
		job2.setOutputValueClass(Text.class);
		job2.submit();

		Job job3 = new Job(configuration, "A6Prediction");
		job3.setJobName("A6PredictionJob3");
		job3.setJarByClass(A6Prediction.class);
		FileInputFormat.addInputPath(job3, new Path(args[2]));
		FileOutputFormat.setOutputPath(job3, new Path(args[3]+"/validate"));
		MultipleOutputs.addNamedOutput(job3, "a6model", TextOutputFormat.class, Text.class, Text.class);
		LazyOutputFormat.setOutputFormatClass(job3, TextOutputFormat.class);
		job3.setMapOutputKeyClass(CompositeGroupKey.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setMapperClass(ValidateMapper.class);
		job3.setReducerClass(ProjectReducer.class);
		job3.setOutputKeyClass(CompositeGroupKey.class);
		job3.setOutputValueClass(Text.class);
		job3.submit();

		while(!job.isComplete() || !job2.isComplete() || !job3.isComplete()){}
		return 0;
	}

	/**
	 * Mapper code
	 * key is year and month
	 * value is: Origin, Destination, CrsDepartureTime, FlDate, FlNum, 
	 *           DayOfMonth, DayOfWeek, delay, DaysToHoilday.
	 * 
	 * */
	public static class ProjectMapper extends Mapper<LongWritable, Text, CompositeGroupKey, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (key.get() > 0) {
				String[] flightDetails = null;
				CSVParser parser = new CSVParser(); 
				flightDetails = parser.parseLine(value.toString());
				String[] testflight=new String[110];
				//check for the test data 
				if(flightDetails.length == 112) System.arraycopy(flightDetails, 1, testflight, 0, 110);
				else testflight = flightDetails;
				if (testflight.length == 110) {
					try {
						AirlineDetails airline=null;
						if(context.getJobName().equals("A6PredictionJob1")) {
							airline = new AirlineDetails(testflight);
							sanityCheck(airline);
						}
						// get the details for the test file
						else airline = new AirlineDetails(testflight, true);
						String year = airline.getYear();
						String month = airline.getMonth();
						int delay = airline.getArrivalDelayMinutes() > 0 ? 1:0;
						Date date = toDateChange(airline.getFlDate());
						String modelData = airline.getOrigin() + SEPARATOR + airline.getDestination() + SEPARATOR
								+ airline.getCarrier()+SEPARATOR + airline.getCrsDepartureTime() + SEPARATOR 
								+ airline.getFlDate() + SEPARATOR + airline.getFlNum() + SEPARATOR 
								+ airline.getDayOfMonth() + SEPARATOR + airline.getDayOfWeek() + SEPARATOR + delay 
								+ SEPARATOR + Utils.closerDate(date,Utils.getHolidays(date));
						CompositeGroupKey compositekey = new CompositeGroupKey(year,month);
						context.write(compositekey, new Text(modelData));
					} catch (InvalidFormatException | InsaneInputException | ParseException | NumberFormatException | 
							ArrayIndexOutOfBoundsException e){} 
				}
			}
		}
	}

	/**
	 * Mapper code
	 * Mapper is for validate file. Key here is year and month and value is the content of file seperated by tab
	 * */
	public static class ValidateMapper extends Mapper<LongWritable, Text, CompositeGroupKey, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (key.get() > 0) {
				String[] v = null;
				CSVParser parser = new CSVParser(); 
				v = parser.parseLine(value.toString());
				String[] keyArr = v[0].split("_");
				String date = keyArr[1];
				CompositeGroupKey compositekey=new CompositeGroupKey(date.split("-")[0],date.split("-")[1]);
				context.write(compositekey, new Text(v[0]+SEPARATOR+v[1]));
			}
		}
	}

	/**
	 * Reducer code
	 * */
	public static class ProjectReducer extends Reducer<CompositeGroupKey, Text, CompositeGroupKey, Text> {
		private MultipleOutputs<CompositeGroupKey, Text> output;
		@Override
		public void setup(Context context){
			output = new MultipleOutputs<CompositeGroupKey, Text>(context);
		}
		@Override
		public void reduce(CompositeGroupKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text v : values) {
				output.write("a6model",key,v,key.year+"_"+key.month.replaceAll("^0",""));
			}
		}
		@Override
		protected void cleanup(
				Reducer<CompositeGroupKey, Text, CompositeGroupKey, Text>.Context context)
						throws IOException, InterruptedException {
			output.close();
			super.cleanup(context);
		}
	}

	/**
	 * @param d
	 * Method takes string date and parses it to yyyy-MM-dd format
	 * */
	private static Date toDateChange(String d) throws ParseException {
		Date date = null;
		if(StringUtils.isNotEmpty(d)){
			if (d.contains("/")) {
				d.replace("/", "-");
				date = form.parse(d);
			}
			else if (d.contains("-")) 
				date = form.parse(d);
		}
		return date;
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
}