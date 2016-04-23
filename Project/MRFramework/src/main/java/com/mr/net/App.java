package com.mr.net;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.mr.mapreduce.Mapper;
import com.mr.mapreduce.Reducer;
import com.mr.util.AWSUtility;
import com.mr.util.ReadProperty;

/**
 * 
 * @author Shakti
 *
 * This is an implementation of Mapreduce
 * 
 * Concepts and APIs Used :
 * 		1. java.net.Socket 
 * 		2. AWS SDK for S3 file management and EC2 instances
 * 		3. Java Generics Implementation
 * 		4. Jsch for Shell Scripting in Java
 * 		5. Log4j for logging
 * 
 * How API works:
 * 		1. On running a jar which implements this API, it calls App's run Method 
 * 		2. This invokes the createInstances method, which creates the specified number of instances 
 * 		3. It then copies the jar on all of them and starts them 
 * 		4. The Master starts the master server on node 0, and waits for 5 seconds just for safety.
 * 		5. Then nodes start and polls the server on the port for their files to read
 * 		6. Master reads the files from S3, and distribute file names among the nodes
 * 		7. Nodes read alloted files, and calls mapper function
 * 		8. Output is sorted globally and shuffled
 * 		9. Each node reads alloted keys(files) and calls reducer
 * 	   10. Reducer output is written to output folder in S3    
 * 
 */
public class App<KEY extends Comparable<KEY>,VALUE> {

	static Logger logger = Logger.getLogger(App.class.getName());
	public static int nodeIdentity; 
	public static Map<Integer, String> ipmap = new HashMap<Integer, String>();
	public static String masterip = null;
	public static List<String>[] filesList = null;
	public static boolean globalPivotsCompleteFlag = false;
	public static AmazonS3 s3;
	public static String bucket = "";
	public static String key = "";
	public static String pwd = "";
	public static String inputPath = "";
	public static String ipFile = "";
	public static String output = "";
	Mapper<KEY, VALUE> mapper = null;
	Reducer<KEY, VALUE> reducer = null;
	
	/**
	 * Functionality :
	 * 
	 * 1. Reads system properties from command line parameters (id,type) 
	 * 		and calls appropriate method
	 * 2. Calls createInstance method to create desired instances on EC2
	 * 3. Reads the IpAddresses.txt file to read all instances that are created
	 * 4. Calls Master node to create Server and listen for sockets
	 * 5. Calls Node to start client servers and start mapreduce operation.
	 * 
	 * @throws Exception
	 */
	public void run() throws Exception {
		String type=System.getProperty("type");
		String id=System.getProperty("id");
		ReadProperty.readFromFile("config.properties");
		s3 = new AmazonS3Client(new BasicAWSCredentials(key, pwd));
		if(type == null) CreateInstances.initialize();
		else if(type.equals("master")) {
			AWSUtility.s3ReadIpFile(bucket, ipFile);
			filesList  = AWSUtility.s3ReadBuckets(bucket, inputPath);
			logger.info("array length= " + filesList.length);
			new MasterServer<KEY, VALUE>().startServer();
		} else if (type.equals("slave")){
			nodeIdentity = Integer.parseInt(id);
			new Node<KEY, VALUE>(mapper, reducer).run();
		}
	}
	
	/**
	 * Constructor for App class
	 * @param mapper : mapper class to set
	 * @param reducer : reducer class to set
	 */
	public App(Mapper<KEY, VALUE> mapper, Reducer<KEY, VALUE> reducer) {
		super();
		this.mapper = mapper;
		this.reducer = reducer;
	}
}
