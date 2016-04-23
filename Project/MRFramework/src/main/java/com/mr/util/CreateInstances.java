package com.mr.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.DeleteSecurityGroupRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceState;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.mr.net.App;
import com.opencsv.CSVParser;

/**
 * Creates security group and EC2 instances within security group using the AWS
 * credential of the user. Writes the instance data into S3 bucket which is
 * later used to terminate the instances and delete the security group. User is
 * required to mention type of the number of the instance, EC2 instance, action
 * to start/stop instance, region of the instance and S3 bucket name
 * 
 * @author Kartik Mahaley
 * @version 1.0
 */
public class CreateInstances {
	public static Logger log = Logger.getLogger(CreateInstances.class.getName());
	public static AmazonEC2 ec2;
	public static AmazonS3 s3;
	public static List<String> activeInstances = new LinkedList<>();
	public static List<String> activePublicDnsName = new LinkedList<>();
	public static Map<Integer, String> mapOfInstances = new HashMap<Integer, String>();
	public static CSVParser csv_parser = new CSVParser();
	public static String ami = "ami-08111162";
	public static String securityGroupName = "JavaSecurityGroup";
	public static String bucketName = "";
	public static String pemKeys = "";
	public static String secretKey = "";
	public static String accessKey = "";
	public static String instanceType = "";
	public static int numberOfEC2Instances = 1;
	public static String action;
	public static String pemKeysPath;
	public static String jarPath; 
	
	/**
	 * This function takes input as "start/stop, number of instance, secret key,
	 * access key, region, bucket name, instance type". This will start
	 * instances, upload file to s3 bucket with the instances data and stop
	 * instances
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void initialize() throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		bucketName=App.bucket;
		secretKey=App.key;
		accessKey=App.pwd;
		startUp();
		if (action.toLowerCase().equals("start")) {
			createInstances(numberOfEC2Instances);
			writeIPsIntoS3Bucket();
			copyJarToInstances();
			runJarInInstances();
		} else if (action.toLowerCase().equals("stop")) {
			readIPsFromS3Bucket();
			terminateInstances();
			deleteSecurityGroup();
		} else {
			log.info("please check the configuration file prioperties ");
			System.exit(-1);
		}
	}

	
	/**
	 * Sets amazon EC2 and S3 client using aws credentials using secret key and
	 * access key
	 * 
	 * @throws Exception
	 */
	private static void startUp() throws Exception {
		s3 = new AmazonS3Client(new BasicAWSCredentials(secretKey, accessKey));
		ec2 = new AmazonEC2Client(new BasicAWSCredentials(secretKey, accessKey));
	}

	/**
	 * Creates security group and instances in it.
	 * 
	 * @param numberOfInstances
	 * @throws InterruptedException
	 */
	private static void createInstances(int numberOfInstances) throws InterruptedException {
		CreateSecurityGroupRequest secGrpReq = new CreateSecurityGroupRequest(securityGroupName, "A9 Security group");
		ec2.createSecurityGroup(secGrpReq);
		AuthorizeSecurityGroupIngressRequest r2 = new AuthorizeSecurityGroupIngressRequest();
		r2.setGroupName(securityGroupName);

		/************* the property of http *****************/
		IpPermission permission = new IpPermission();
		permission.setIpProtocol("tcp");
		permission.setFromPort(80);
		permission.setToPort(80);
		List<String> ipRanges = new ArrayList<String>();
		ipRanges.add("0.0.0.0/0");
		permission.setIpRanges(ipRanges);

		/************* the property of SSH **********************/
		IpPermission permission1 = new IpPermission();
		permission1.setIpProtocol("tcp");
		permission1.setFromPort(22);
		permission1.setToPort(22);
		List<String> ipRanges1 = new ArrayList<String>();
		ipRanges1.add("0.0.0.0/0");
		permission1.setIpRanges(ipRanges1);

		/************* the property of https **********************/
		IpPermission permission2 = new IpPermission();
		permission2.setIpProtocol("tcp");
		permission2.setFromPort(443);
		permission2.setToPort(443);
		List<String> ipRanges2 = new ArrayList<String>();
		ipRanges2.add("0.0.0.0/0");
		permission2.setIpRanges(ipRanges2);

		/************* the property of tcp **********************/
		IpPermission permission3 = new IpPermission();
		permission3.setIpProtocol("tcp");
		permission3.setFromPort(0);
		permission3.setToPort(65535);
		List<String> ipRanges3 = new ArrayList<String>();
		ipRanges3.add("0.0.0.0/0");
		permission3.setIpRanges(ipRanges3);

		/********************** add rules to the group *********************/
		List<IpPermission> permissions = new ArrayList<IpPermission>();
		permissions.add(permission);
		permissions.add(permission1);
		permissions.add(permission2);
		permissions.add(permission3);
		r2.setIpPermissions(permissions);
		ec2.authorizeSecurityGroupIngress(r2);
		List<String> groupName = new ArrayList<String>();
		groupName.add(securityGroupName);
		log.info("Number of intances = " + numberOfInstances);
		String amazonMachineImage = ami;
		int minInstanceCount = 1;
		int maxInstanceCount = numberOfInstances;
		RunInstancesRequest runInstancesRequest = new RunInstancesRequest(amazonMachineImage, minInstanceCount,
				maxInstanceCount);
		runInstancesRequest.setInstanceType(instanceType);
		runInstancesRequest.setKeyName(pemKeys);
		runInstancesRequest.setSecurityGroups(groupName);
		RunInstancesResult runInstancesResult = ec2.runInstances(runInstancesRequest);

		/* To make sure instances are running */
		log.info("wait for instances to be in running state");
		List<Instance> resultInstance = runInstancesResult.getReservation().getInstances();
		for (Instance ins : resultInstance) {
			while (getInstanceStatus(ins.getInstanceId()) != 16) {
				Thread.sleep(5000);
			}
			activeInstances.add(ins.getInstanceId());
		}
		log.info("Instances created and running");
	}

	/**
	 * Gives state code of the instance
	 * 
	 * @param instanceId
	 * @return state code of the instance Function receives instanceID as input
	 *         and returns the integer code of the instance state for example 48
	 *         = terminate
	 */
	public static Integer getInstanceStatus(String instanceId) {
		DescribeInstancesRequest describeInstanceRequest = new DescribeInstancesRequest().withInstanceIds(instanceId);
		DescribeInstancesResult describeInstanceResult = ec2.describeInstances(describeInstanceRequest);
		InstanceState state = describeInstanceResult.getReservations().get(0).getInstances().get(0).getState();
		return state.getCode();
	}

	/**
	 * Writing instances data to the s3 bucket with helper function createIPFile
	 * 
	 * @throws IOException
	 */
	private static void writeIPsIntoS3Bucket() throws IOException {
		s3.putObject(new PutObjectRequest(bucketName, "ipaddress.txt", createIPFile()));
		log.info("instances data written in S3 bucket " + bucketName);
	}

	/**
	 * Write all instance data into a file
	 * 
	 * @return File, containing instance data
	 * @throws IOException
	 */
	private static File createIPFile() throws IOException {
		File file = File.createTempFile("ipaddress", ".txt");
		file.deleteOnExit();
		Writer writer = new OutputStreamWriter(new FileOutputStream(file));
		DescribeInstancesRequest request = new DescribeInstancesRequest();
		request.setInstanceIds(activeInstances);
		DescribeInstancesResult res = ec2.describeInstances(request);
		List<Reservation> reservations = res.getReservations();
		List<Instance> instances;
		int count = 0;
		for (Reservation r : reservations) {
			instances = r.getInstances();
			for (Instance ins : instances) {
				log.info(ins.getPublicIpAddress() + "," + ins.getPublicDnsName());
				writer.write(count + "," + ins.getInstanceId() + "," + ins.getPublicIpAddress() + ","
						+ ins.getPublicDnsName() + "\n");
				activePublicDnsName.add(ins.getPublicDnsName());
				mapOfInstances.put(count, ins.getPublicIpAddress());
				count++;
			}
		}
		writer.close();
		return file;
	}

	/**
	 * Copy jar and properties file from given location to EC2 instance /tmp
	 * location.
	 */
	private static void copyJarToInstances() {
		try {
			log.info("Copying Jar to all EC2 instances");
			for (String dns : activePublicDnsName) {
				ProcessBuilder pb1 = new ProcessBuilder("scp", "-i", pemKeysPath+pemKeys+".pem", "-o",
						"StrictHostKeyChecking=no", jarPath, "ec2-user@" + dns + ":/tmp");
				Process p1 = null;
				int error1 = -1;
				do {
					p1 = pb1.start();
					error1 = p1.waitFor();
				} while (error1 != 0);
				log.info(dns + "\tJar  = " + error1 + "\t");
				if (error1 == 0) {
					ProcessBuilder pb2 = new ProcessBuilder("scp", "-i", pemKeysPath+pemKeys+".pem", "-o",
							"StrictHostKeyChecking=no", "config.properties",
							"ec2-user@" + dns + ":/tmp");
					Process p2 = null;
					int error2 = -1;
					do {
						p2 = pb2.start();
						error2 = p2.waitFor();
					} while (error2 != 0);
					log.info("Config file  = " + error2);
					ProcessBuilder pb3 = new ProcessBuilder("scp", "-i", pemKeysPath+pemKeys+".pem", "-o",
							"StrictHostKeyChecking=no", "log4j.properties",
							"ec2-user@" + dns + ":/tmp");
					Process p3 = null;
					int error3 = -1;
					do {
						p3 = pb3.start();
						error3 = p3.waitFor();
					} while (error3 != 0);
					log.info("log4j Config file  = " + error3);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Run commands specific to instance number. 0 stands for master and rest
	 * are slave.
	 */
	private static void runJarInInstances() {
		try {
			log.info("Running Jar on all EC2 instances");
			String user = "ec2-user";
			int port = 22;
			String privateKey = pemKeysPath+pemKeys+".pem";
			String command = "";
			for (Map.Entry<Integer, String> entry : mapOfInstances.entrySet()) {
				JSch jsch = new JSch();
				String host = entry.getValue();
				jsch.addIdentity(privateKey);
				Session session = jsch.getSession(user, host, port);
				log.info("session created.");
				java.util.Properties config = new java.util.Properties();
				config.put("StrictHostKeyChecking", "no");
				session.setConfig(config);
				session.connect();
				Channel channel = session.openChannel("shell");
				if (entry.getKey() == 0)
					command = "cd /tmp \n ls \n java -Dtype=\"master\" -jar "+jarPath+" master &\n ";
				else
					command = "cd /tmp \n ls \n java -Dtype=\"slave\" -Did=\""+entry.getKey()+"\" -jar "+jarPath+" &\n";
				InputStream in = new ByteArrayInputStream(command.getBytes(StandardCharsets.UTF_8));
				channel.setInputStream(in);
				channel.setOutputStream(System.out);
				channel.connect(10 * 1000);
				if(entry.getKey() == 0) {
					Thread.sleep(5000);
				}
			}
		} catch (Exception e) {
			log.error(e.getMessage());
		}

	}

	/**
	 * Read IP addressess file from s3 bucket and their instance ID
	 * 
	 * @throws IOException
	 */
	private static void readIPsFromS3Bucket() throws IOException {
		log.info("Reading  instances data from S3 bucket " + bucketName);
		S3Object s3o = s3.getObject(new GetObjectRequest(bucketName, "ipaddress.txt"));
		BufferedReader reader = new BufferedReader(new InputStreamReader(s3o.getObjectContent()));
		String line = null;
		while ((line = reader.readLine()) != null) {
			log.info(line);
			String[] instanceIDs = csv_parser.parseLine(line);
			activeInstances.add(instanceIDs[1]);
		}
	}

	/**
	 * Terminates list of instances
	 */
	private static void terminateInstances() {
		log.info("Terminating instances");
		TerminateInstancesRequest terminate = new TerminateInstancesRequest(activeInstances);
		ec2.terminateInstances(terminate);
	}

	/**
	 * This function sleeps and wait for instances to get terminated and then
	 * delete the security group
	 * 
	 * @throws InterruptedException
	 */
	private static void deleteSecurityGroup() throws InterruptedException {
		for (String s : activeInstances) {
			while (getInstanceStatus(s) != 48) {
				Thread.sleep(5000);
			}
			log.info(s + " = terminated");
		}
		DeleteSecurityGroupRequest d = new DeleteSecurityGroupRequest().withGroupName(securityGroupName);
		ec2.deleteSecurityGroup(d);
		log.info("Group Deleted");
	}
}