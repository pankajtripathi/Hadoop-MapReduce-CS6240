package com.assignment.A4;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author : Pankaj Tripathi, Kartik Mahaley
 * Class Name : A4RegressionMain.java
 * Purpose : Function checks the number of argument and decide on a job to run
 * Example : A4Regression or GraphPlotter driver calls respective mapreduce job
 */
public class A4Main extends Configured implements Tool {

	public static void main(String[] args) throws Exception{
		System.exit(ToolRunner.run(new A4Main(), args));
	}	

	public int run(String[] args) throws Exception{
		String cheapestCarrier = new A4Regression().runRegression(args);
		String arg[] = new String[]{args[0],args[1]+"-plotter",cheapestCarrier};
		return new GraphPlotter().startPlot(arg);
	}
}
