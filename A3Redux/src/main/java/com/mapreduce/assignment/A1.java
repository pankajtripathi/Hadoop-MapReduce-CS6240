package com.mapreduce.assignment;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
/**
 * Created on 2/5/16.
 * author: Pankaj Tripathi, Kartik Mahaley
 */
public class A1 {

	static TreeMap<String,Double> avgPrice = new TreeMap<String, Double>();
	static Set<String> activeIn2015 = new TreeSet<String>();

	public A1(String inputDir,String outputDir,String opr) {
		List<SanityCheckProcess> sanityCheckProcesses=new ArrayList<SanityCheckProcess>();
		File file=new File(inputDir);
		File listFile[]=file.listFiles();
		// Implementing threading with number of threads based on available processors
		ExecutorService executor= Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		//Sanity check for each files
		SanityCheckProcess obj;
		for (File f : listFile) {
			obj=new SanityCheckProcess(f);
			sanityCheckProcesses.add(obj);
			executor.submit(obj);
		}
		executor.shutdown();
		try {
			if(executor.awaitTermination(5, TimeUnit.MINUTES)){
				int K=0,F=0;
				for(SanityCheckProcess s:sanityCheckProcesses){
					K=K+s.badData;
					F=F+s.goodData;
					activeIn2015=s.activeIn2015;
					TreeMap<String,List<Double>> map=s.map;
					if(opr.equals("mean")){
						calculateMean(map);
					}
					else if(opr.equals("median")){
						calculateMedian(map);
					}
					else System.exit(0);
				}
				displayData(outputDir,opr);
			}
			else
				System.out.println("Timeout");
		}
		catch (InterruptedException e){
			e.printStackTrace();
		}
	}
	/**
	 * @param opr -> operation whether it is mean or median
	 * @param outfoldr -> the output folder path
	 *        Method writes the carrier month and mean or median on a text file
	 *        at he outfoldr path
	 * */
	private static void displayData(String outputDir,String opr) {
		try{
			File outputFolder = new File(outputDir);
			if (!outputFolder.exists()) {
				outputFolder.mkdir();
			}
			File outputTextFile =new File(outputFolder+"/"+"outputDataMultiThread"+opr+".txt");
			if (!outputTextFile.exists()) {
				outputTextFile.createNewFile();
			}
			FileWriter fw1 = new FileWriter(outputTextFile.getAbsoluteFile());
			BufferedWriter bw1 = new BufferedWriter(fw1);
			for (Map.Entry<String,Double> entry:avgPrice.entrySet()) {
				String keyval[] = entry.getKey().split("\t");
				String carrier = keyval[1];
				String month = keyval[0];
				if(activeIn2015.contains(carrier)) {
					bw1.write(month+"\t"+carrier+"\t"+new DecimalFormat("##.##")
					.format(entry.getValue())+"\n");
				}
			}
			bw1.close();
		}
		catch(IOException ie){}
	}

	/**
	 * @param map -> map with all the values from al csv files
	 * Retrieve the carrier with month as key and list of prices as values from map.
	 * calculate avg of the prices and then add it to new tree map of type
	 * TreeMap<String,Double> which will have carrier and month as key and mean price as value.
	 * */
	public static void calculateMean(TreeMap<String, List<Double>> map){
		for (Map.Entry<String,List<Double>> entry:map.entrySet()) {
			String carrierandmonth = entry.getKey();
			int size=entry.getValue().size();
			double avg=0,mean=0;
			for (Double val:entry.getValue()) {
				avg=avg+val;
			}
			mean=avg/size;
			avgPrice.put(carrierandmonth,mean);
		}
	}
	/**
	 * @param map -> map with all the values from all .csv files
	 * Retrieve the carrier with month as key and list of prices as values from map.
	 * calculate median of the prices and then add it to new tree map of type
	 * TreeMap<String,Double> which will have carrier and month as key and median price as value.
	 * */
	public static void calculateMedian(TreeMap<String, List<Double>> map){
		double median=0;
		for (Map.Entry<String,List<Double>> entry:map.entrySet()) {
			String monthandcarrier = entry.getKey();
			Collections.sort(entry.getValue());
			int length=entry.getValue().size();
			int middle=length/2;
			if(length%2==1)
				median=entry.getValue().get(middle);
			else
				median=(entry.getValue().get(middle-1)+entry.getValue().get(middle))/2.0;
			avgPrice.put(monthandcarrier,median);
		}
	}
}

