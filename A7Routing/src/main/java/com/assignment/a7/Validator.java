package com.assignment.a7;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;


/**
 * @author Shakti Patro
 * @author Pankaj Tripathi
 * @author Kartik Mahaley
 * @author Chen Bai
 * 
 * Functionality :
 * 1. This class is run on local machine
 * 2. This program takes the validator file and the predicted file as input
 * 3. generates the accuracy and scoring data 
 * 4. If missed connection is predicted a penalty of 100 hours is added. 
 */
public class Validator {
	final static String SEP = ",";
	static Map<String, Integer> predictionMap = new HashMap<String, Integer>();
	static Map<String, Integer> validationMap = new HashMap<String, Integer>();
	static long missed=0, totalCount=0, predictedscore=0;

	/**
	 * @param args - prediction, validator 
	 * 1. Reads files
	 * 2. Calculates accuracy
	 * 3. Writes to file "score.txt" with duration
	 */
	public static void validate(String[] args) {
		readFile("prediction", args[0]);
		readFile("validation", args[1]);
		accuracy();
		try {
			File file = new File("score.txt");
			if (!file.exists()) {
				file.createNewFile();
			}
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			for(Entry<String, Integer> e : predictionMap.entrySet()){
				bw.write(e.getKey()+SEP+e.getValue()+"\n");
			}
			bw.write(missed + SEP + (totalCount-missed) + SEP +totalCount );
			bw.close();
		} catch (IOException e) {}
	}

	/**
	 * @param s1 : prediction file
	 * @param s2 : validation file
	 * Checks if the prediction is in the validator file which is a list of missed connections
	 * if present, add 100 hours to duration as penalty
	 *  
	 */
	static public void readFile(String s1, String s2) {
		String type = s1;
		String fileName = s2;
		String line = null;
		BufferedReader bufferedReader = null;
		GZIPInputStream gZip = null;
		try {
			if (type.equals("prediction")) {
				FileReader in = new FileReader(fileName);
				bufferedReader = new BufferedReader(in);
			} else {
				gZip = new GZIPInputStream(new FileInputStream(fileName));
				Reader in = new InputStreamReader(gZip);
				bufferedReader = new BufferedReader(in);
			}
			while ((line = bufferedReader.readLine()) != null) {
				String[] stringArray = line.split(SEP);
				if (type.equals("prediction")) {
					String key = stringArray[0] + SEP + stringArray[1] + SEP + stringArray[2] + SEP + stringArray[3]
							+ SEP + stringArray[4] + SEP + stringArray[5] + SEP + stringArray[6];
					Integer value = Integer.parseInt(stringArray[7]);
					predictionMap.put(key, value);
				} else {
					String key = stringArray[0] + SEP + stringArray[1] + SEP + stringArray[2] + SEP + stringArray[3]
							+ SEP + stringArray[4] + SEP + stringArray[5] + SEP + stringArray[6];
					validationMap.put(key, 1);
				}
			}
			bufferedReader.close();
			if(gZip!= null) gZip.close();
		} 
		catch (IOException ex) {}
	}

	/**
	 * Calculates accuracy of the model  
	 * (Predicted Missed, Total, Predicted Connection not missed)
	 */
	static public void accuracy(){
		for(Entry<String, Integer> e : predictionMap.entrySet()){
			if(validationMap.containsKey(e.getKey())){
				missed++;
				predictionMap.put(e.getKey(), e.getValue()+100);
			}
			totalCount++;
		}
	}
}