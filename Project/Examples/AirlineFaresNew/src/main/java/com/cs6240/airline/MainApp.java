package com.cs6240.airline;
/**
 * @author : Shakti Patro 
 * Its the main class
 * Also calculates the time taken for the complete job.
 * 
 */

/*
 * This class is used to read the arguments and call the appropriate functions
 * It calls either single threaded , or multi threaded or psedo modes.
 * 
 */
public class MainApp {
	
	public static void main(String[] args) throws Exception {
		MapReduceJob.run();
	}

}
