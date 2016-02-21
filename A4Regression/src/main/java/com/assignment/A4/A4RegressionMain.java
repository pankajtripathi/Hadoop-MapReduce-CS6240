package com.assignment.A4;
/*
 * @author : Pankaj Tripathi, Kartik Mahaley
 * Class Name : A4RegressionMain.java
 */
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.util.ToolRunner;

public class A4RegressionMain {
	/*
	 * @author : Pankaj Tripathi, Kartik Mahaley
	 * Class Name : A4RegressionMain.java
	 * Purpose : Function checks the number of argument and decide on a job to run
	 * Example : A4Regression or GraphPlotter driver calls respectives mapreduce job
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.out.println("usage: [jobtype] [input] [output] [carrier]");
			System.exit(-1);
		}
		String jobType = args[0].toLowerCase();
		String inputDir = args[1];
		String outputDir = args[2];
		String jobOneArgs[]={jobType,inputDir,outputDir};
		
		if(jobType.equals("regression")){
			int res = ToolRunner.run(new A4Regression(), jobOneArgs);
			System.exit(res);
		}else{
			String carrier = null;
	        String fileName = args[3];
	        String line = null;
	        try {
	            FileReader fileReader = new FileReader(fileName);
	            BufferedReader bufferedReader = new BufferedReader(fileReader);

	            while((line = bufferedReader.readLine()) != null) {
	            	carrier = line.replace("\"", "");
	            }
	            String jobTwoArgs[]={jobType,inputDir,outputDir,carrier};
	        	int res = ToolRunner.run(new GraphPlotter(), jobTwoArgs);
	        	bufferedReader.close(); 
				System.exit(res);      
	        }
	        catch(FileNotFoundException ex) {
	            System.err.println("Unable to open file '" + fileName + "'");                
	        }
	        catch(IOException ex) {
	            System.err.println("Error reading file '"  + fileName + "'");                  
	        }		
		}
	}
}
