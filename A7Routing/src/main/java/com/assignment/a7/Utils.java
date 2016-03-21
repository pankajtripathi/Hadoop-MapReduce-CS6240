package com.assignment.a7;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import quickml.data.AttributesMap;
import quickml.data.instances.ClassifierInstance;
import quickml.supervised.ensembles.randomForest.randomDecisionForest.RandomDecisionForest;

import com.google.common.collect.Lists;
import com.opencsv.CSVParser;

/**
 * @author Shakti Patro
 * @author Pankaj Tripathi
 * @author Kartik Mahaley
 * @author Chen Bai
 * 
 * @idea: from Chintan Pathak (A6)  
 * Functionality :
 * This is the Utils class for creating models, serializing them and deserializing them. 
 */

/*
 * This class provides various utility functions to use in doing the required
 * processing
 */
public class Utils {
	/**
	 * Method to create a data set out of objects of class 
	 * ModelingCompositeKey and ModelingCompositeValue that
	 * is to be used further to create a prediction model
	 * 
	 * @param list
	 * @return List<ClassifierInstance>
	 * @throws IOException 
	 */
	public static List<ClassifierInstance> loadDataSet(Iterable<Text> list) throws IOException {
		List<ClassifierInstance> instances = Lists.newLinkedList();
		AttributesMap attributes;
		CSVParser csv_parser = new CSVParser();
		Set<String> missedClasses = new HashSet<String>();
		for (Text item : list) {
			String[] line = csv_parser.parseLine(item.toString());
			if(line.length != 15) continue;
			attributes = AttributesMap.newHashMap();
			attributes.put("carrier", line[0]);
			attributes.put("month", line[2]);
			attributes.put("day_of_month", line[3]);
			attributes.put("day_of_week", line[4]);
			attributes.put("origin", line[5]);
			attributes.put("dest", line[6]);
			attributes.put("arr_time", line[10]);
			attributes.put("elapsed_time", line[11]);
			attributes.put("dist_group", line[12]);
			attributes.put("missed", line[14]);
			instances.add(new ClassifierInstance(attributes, line[14]));
			missedClasses.add(line[14]);
		}
		if(missedClasses.size() < 2) return null;
		return instances;
	}

	/**
	 * This method serializes the passed object to the folder serializedObjects/
	 * in the bucket set by the configuration parameter "bucket.name"
	 *  
	 * @param key
	 * @param object
	 * @param conf
	 */
	public static <T> void serializeObject(Text key, T object, Configuration conf) {
		try {
			String[] keys =  key.toString().split(ModelJob.SEPATRATOR);
			FileSystem fs = FileSystem.get(new URI(conf.get("bucket.name")), conf);
			DataOutputStream outS = fs.create(new Path("serializedObjects/" + keys[0]+"_"+keys[1]));
			ObjectOutputStream out = new ObjectOutputStream(outS);
			out.writeObject(object);
			out.close();
			outS.close();
		} catch (URISyntaxException | IOException ex) {
			System.err.println("Exception with message : " + ex.getLocalizedMessage());
		}
	}

	/**
	 * This method checks if the required file exists inside the folder serializedObjects/
	 * of the bucket set by the configuration parameter "bucket.name"
	 *  
	 * @param key
	 * @param object
	 * @param conf
	 */
	public static boolean forestExists(String key, Configuration conf) {
		try {
			FileSystem fs = FileSystem.get(new URI(conf.get("bucket.name")), conf);
			return fs.exists(new Path("serializedObjects/" + key));
		} catch (IOException | URISyntaxException e) {}
		return false;
	}

	/**
	 * This method deserializes the object stored inside the folder serializedObjects/
	 * of the bucket set by the configuration parameter "bucket.name" with the required
	 * file name
	 *  
	 * @param key
	 * @param object
	 * @param conf
	 */
	public static RandomDecisionForest deserializeObject(String key, Configuration conf) {
		Object obj = null;
		try {
			FileSystem fs = FileSystem.get(new URI(conf.get("bucket.name")), conf);
			DataInputStream inS = fs.open(new Path("serializedObjects/" + key));
			ObjectInputStream in = new ObjectInputStream(inS);

			obj = in.readObject();
			in.close();
			inS.close();
		} catch (IOException i) {
			i.printStackTrace();
		} catch (ClassNotFoundException c) {
			c.printStackTrace();
		} catch (URISyntaxException ex) {
			System.err.println("URI Syntax Exception with message : " + ex.getLocalizedMessage());
		}
		return (RandomDecisionForest) obj;
	}
}
