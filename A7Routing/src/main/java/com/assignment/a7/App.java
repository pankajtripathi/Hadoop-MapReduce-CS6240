package com.assignment.a7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Shakti Patro
 * @author Pankaj Tripathi
 * @author Kartik Mahaley
 * @author Chen Bai
 * 
 * Functionality :
 * 1. Main function for the program
 * 2. takes in arguments starting with either "predict" or "validate"
 * 3. Calls respective methods and records the execution times(prints to console)
 */

public class App {

	public static void main(String[] args) throws Exception {

		long start = System.currentTimeMillis();
		if(args[0].equals("predict")){
			if (args.length < 6) {
				System.out.println("usage: predict [train] [test] [request] [output] [bucket]");
				System.exit(-1);
			}
			Configuration conf = new Configuration();
			conf.set("mapreduce.output.textoutputformat.separator", ",");
			conf.set("bucket.name", args[5]);
			conf.set("job.type", "train");
			ToolRunner.run(conf, new AirlineConnectionsJob(), new String[] { args[1], args[5]+"/train_connections_output" });
			ToolRunner.run(conf, new ModelJob(), new String[] { args[5]+"/train_connections_output" , args[5]+"/model_output" });
			long stop1 = System.currentTimeMillis();
			System.out.println("Train Time : " + (float) (stop1 - start) / 60000);
			conf.set("job.type", "test");
			ToolRunner.run(conf, new AirlineConnectionsJob(), new String[] { args[2], args[5]+"/test_connections_output" });
			ToolRunner.run(conf, new PredictionJob(), new String[] { args[5]+"/test_connections_output" , args[3], args[4] });
			long stop2 = System.currentTimeMillis();
			System.out.println("Predict Time : " + (float) (stop2 - stop1) / 60000);

		} else if(args[0].equals("validate")) {
			if (args.length < 3) {
				System.out.println("usage: validate [predict] [validate]");
				System.exit(-1);
			}
			Validator.validate(new String[] {args[1] , args[2]});
			long stop = System.currentTimeMillis();
			System.out.println("Validate Time : " + (float) (stop - start) / 60000);

		} else {
			System.out.println("Please enter predict or validate only");
			System.out.println("usage: predict [train] [test] [request] [output] [bucket]");
			System.out.println("usage: validate [predict] [validate]");
			System.exit(-1);
		}
	}
}
