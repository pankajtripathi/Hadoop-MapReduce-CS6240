package com.spark.airline;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import com.opencsv.CSVParser;
import com.spark.airline.Utils.RecordIndex;


/**
 * 
 * @author Shakti Patro, Kartik Mahaley, Chen Bai, Pankaj Tripathi
 * 
 * This is a Spark application which does the follwing jobs
 * 
 * 1. Reads and validates airline data
 * 2. Groups JavaPairRDDs for (Year Carrier) , (ElaspsedTime, Price)
 * 3. Applier Simple Linear regression on each key 
 * 4. Predicts Price for N(input elapsed time, defaults to 1)
 * 5. Calcultes cheapest Airline
 * 6. Creates JavaRDD for weekly prices for the cheapest airline
 * 7. Saves the RDD as test file in output folder 
 *   
 *
 */
public class SparkApp {
	static String SEPARATOR = ",";
	static CSVParser csv_parser = new CSVParser();
	static DecimalFormat formatter=new DecimalFormat("##.##");
	
	@SuppressWarnings("serial")
	public static void predict(String input, String output, final Double N) {
		long start = System.currentTimeMillis();
		SparkConf conf = new SparkConf().setAppName("SparkJob1").set("spark.executor.memory", "1g");
		final JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> saneAirlinesData = sc.textFile(input).filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				String[] record = csv_parser.parseLine(s);
				return (!Utils.stringToBoolean(record[Utils.RecordIndex._47_CANCELLED]) && 
						Utils.isRecordSane(record));
			}
		});
		long stop1 = System.currentTimeMillis();
		System.out.println("Phase 1(SaneData) : " + (float) (stop1 - start) / 60000);
		
		JavaPairRDD<String, Iterable<double[]>> train = saneAirlinesData.map(new Function<String, String[]>() {
			public String[] call(String line) throws Exception {
				String[] flightDetails = csv_parser.parseLine(line.toString());
				return new String[]{flightDetails[RecordIndex._00_YEAR], flightDetails[RecordIndex._08_CARRIER_ID], 
						flightDetails[RecordIndex._51_ACT_ELAPSED_TIME], flightDetails[RecordIndex._109_AVG_TICKET_PRICE]};
			}
		}).mapToPair(new PairFunction<String[], String, double[]>() {
			@Override
			public Tuple2<String, double[]> call(String[] airline) throws Exception {
				double price = Double.valueOf(airline[3]);
				double elapsedTime = Double.valueOf(airline[2]);
				return new Tuple2<String, double[]>(airline[0]+","+airline[1] , 
						new double[]{elapsedTime,price});
			}
		}).groupByKey();
		long stop2 = System.currentTimeMillis();
		System.out.println("Phase 2(Pairs) : " + (float) (stop2 - stop1) / 60000);
		
		JavaPairRDD<String, String> predictions = train.mapValues(new Function<Iterable<double[]>, String>() {
			@Override
			public String call(Iterable<double[]> values) throws Exception {
				SimpleRegression sRegression = new SimpleRegression();
				for(double[] d: values){
					sRegression.addData(d[0],d[1]);
				}
				double estimatedPrice = sRegression.predict(N);
				return formatter.format(estimatedPrice);
			} 
		});
		
		Map<String, String> cheapest = new HashMap<String, String>();
		Map<String, String> carrierYearPrices = predictions.collectAsMap();
		for (Entry<String, String> e : carrierYearPrices.entrySet()) {
			String[] year_carrier = e.getKey().split(",");
			String year = year_carrier[0];
			String carrier = year_carrier[1];
			String price = e.getValue();
			
			if(!cheapest.containsKey(year)) {
				cheapest.put(year, carrier + "," + price);
			}else{
				String carrier_price = cheapest.get(year);
				if (Double.valueOf(carrier_price.split(",")[1]) > Double.valueOf(price))  cheapest.put(year, carrier + "," + price); 
			} 
		}
		Map<String, Integer> cheapestCount = new HashMap<String, Integer>();
		for (Entry<String, String> entry : cheapest.entrySet()) {
			String carrier = entry.getValue().split(",")[0];
			if(cheapestCount.containsKey(carrier)) cheapestCount.put(carrier, cheapestCount.get(carrier)+1);
			else cheapestCount.put(carrier, 1);
		}
		TreeMap<Integer, String> sortedWinnerMap = new TreeMap<Integer, String>(Collections.reverseOrder());
		for(Map.Entry<String, Integer> entry : cheapestCount.entrySet())
			sortedWinnerMap.put(entry.getValue(), entry.getKey());
		final String cheaperstAirline  = sortedWinnerMap.firstEntry().getValue();
		long stop3 = System.currentTimeMillis();
		System.out.println("Phase 3(Model Prediction & cheapest) : " + (float) (stop3 - stop2) / 60000);

		JavaPairRDD<String, Iterable<Double>> weeklyPrices = saneAirlinesData.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				String[] record = csv_parser.parseLine(s);
				return record[RecordIndex._08_CARRIER_ID].equals(cheaperstAirline);
			}
		}).mapToPair(new PairFunction<String, String, Double>() {
			@Override
			public Tuple2<String, Double> call(String airline) throws Exception {
				String[] record = csv_parser.parseLine(airline);
				String week = getWeekOfDate(record[RecordIndex._05_FLIGHT_DATE]);
				double price = Double.valueOf(record[RecordIndex._109_AVG_TICKET_PRICE]);
				return new Tuple2<String, Double>(record[RecordIndex._08_CARRIER_ID] + "," + record[RecordIndex._00_YEAR] + 
						"," + week, price);
			}
		}).groupByKey();
		
		// Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
	    JavaPairRDD<String, String> medians = weeklyPrices.mapValues(new Function<Iterable<Double>, String>() {
			@Override
			public String call(Iterable<Double> values) throws Exception {
				List<Double> priceValues=new ArrayList<Double>();
				for(Double d: values){
					priceValues.add(d);
				}
				Double median=getMedian(priceValues);
				return formatter.format(median);
			} 
	    });
	    medians.saveAsTextFile(output);
		sc.close();
		long stop4 = System.currentTimeMillis();
		System.out.println("Phase 4(Median and save) : " + (float) (stop4 - stop3) / 60000);
	}

	/**
	 * Main method for the application
	 * 1. checks the inputs and output arguments 
	 * 2. Calls predict method with arguments 
	 * 
	 * @param args : <INPUT> <OUTPUT> <N>[default 1]
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		if (args.length < 2) {
			System.out.println("Usage: <INPUT> <OUTPUT> <N>[default 1]");
			System.exit(0);
		}
		//Input and output path
		String input_dir = args[0];
		String output_dir = args[1];
		if (input_dir != null && input_dir.startsWith(Utils.Arguments.INPUT_DIR_PREFIX)) 
			input_dir = input_dir.substring(Utils.Arguments.INPUT_DIR_PREFIX.length());
		if (output_dir != null && output_dir.startsWith(Utils.Arguments.OUTPUT_DIR_PREFIX))
			output_dir = output_dir.substring(Utils.Arguments.OUTPUT_DIR_PREFIX.length());
		Double N = 1.0;
		try{
			N = Double.valueOf(args[2]);	
		}catch(NumberFormatException | ArrayIndexOutOfBoundsException e) {
			System.err.println("<N> Should be a number .Using 1 for calculations.");
			N = 1.0;
		}
		predict(input_dir, output_dir, N);
	}

	/**
	 * @author : Kartik Mahaley, Pankaj Tripathi , Shakti Patro 
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
	
	/**
	 * @author : Kartik Mahaley, Pankaj Tripathi , Shakti Patro
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


}