package com.mr.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.mr.net.App;
import com.mr.net.CreateInstances;

/**
 * Reads Property file and sets to variables
 * 
 * @author Shakti
 *
 */
public class ReadProperty {

	static Logger logger = Logger.getLogger(ReadProperty.class.getName());
	public static void readFromFile(String fileName) {
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = new FileInputStream(fileName);
			// load a properties file
			prop.load(input);

			// properties in App
			App.bucket = prop.getProperty("bucket");
			App.inputPath = prop.getProperty("inputfile");
			App.key = prop.getProperty("key");
			App.pwd = prop.getProperty("password");
			App.ipFile = prop.getProperty("ipfile");
			App.output = prop.getProperty("output");
			
			// properties in CreateInstances
			CreateInstances.action=prop.getProperty("action");
			CreateInstances.pemKeys = prop.getProperty("pem");
			CreateInstances.pemKeysPath = prop.getProperty("pemKeysPath");
			CreateInstances.jarPath=prop.getProperty("jarPath");
			CreateInstances.instanceType = prop.getProperty("instancetype");
			CreateInstances.numberOfEC2Instances = Integer.valueOf(prop.getProperty("instancenumber"));
		} catch (IOException io) {
			logger.error(io.getMessage());
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					logger.error(e.getMessage());
				}
			}
		}

	}
}
