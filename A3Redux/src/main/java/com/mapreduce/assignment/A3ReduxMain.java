package com.mapreduce.assignment;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class A3ReduxMain {
	public static void main(String[] args) throws Exception {

		if (args.length != 4) {
			System.err.println("Usage: A3ReduxMain <-s/-m/-h> <input path> <output path> <mean/median/fastmedian>");
			System.exit(-1);
		}

		String mode = args[0].toLowerCase();
		String inputDir = args[1];
		String outputDir = args[2];
		String opr = args[3].toLowerCase();
		String hadoopargs[]={inputDir,outputDir,opr};

		if (!mode.equals("-s") && !mode.equals("-m") && !mode.equals("-h")) {
			System.out.println("Valid 1st argument are -s for single thread run, -m for multi thread run"
					+ " and -h psuedo distributed or emr hadoop mode");
			System.exit(-1);
		}

		if ((mode.equals("-s") || mode.equals("-m")) && (! new File(inputDir).isDirectory())) {
			System.out.println("Input directory or output directory is not valid directory");
			System.exit(-1);
		}
		if (!opr.equals("mean") && !opr.equals("median") && !opr.equals("fastmedian")) {
			System.out.println("Valid 4th argument should be either mean/median/fastMedian");
			System.exit(-1);
		}

		switch(mode) {
		case "-s":
			new A0(inputDir,outputDir,opr);
			break;
		case "-m":
			new A1(inputDir,outputDir,opr);
			break;
		case "-h":
			int res = ToolRunner.run(new Configuration(), new AirlineFare(), hadoopargs);
			System.exit(res);
			break;
		default:
			System.out.println("Valid 1st argument are -s for local sequential run, -p for local parallel run"
					+ " and -h psuedo distributed or emr hadoop mode");
			System.exit(-1);
		}
	}
}
