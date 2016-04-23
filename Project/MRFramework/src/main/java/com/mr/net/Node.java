package com.mr.net;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.mr.io.Collector;
import com.mr.io.Tuple;
import com.mr.mapreduce.Mapper;
import com.mr.mapreduce.Reducer;
import com.mr.util.AWSUtility;

/**
 * @author Pankaj Tripathi 
 * @author Shakti Patro 
 * 
 * Functionality:
 * Fetches the data and IP from the files and then calls for sorting the data between the nodes.
 * Calls map and reduce methods 
 * Writes intermediate files and final files on s3
 * 
 */

@SuppressWarnings("unchecked")
public class Node<KEY extends Comparable<KEY>,VALUE >{
	static Logger logger = Logger.getLogger(Node.class.getName());
	Collector<KEY, VALUE> localData = new Collector<KEY,VALUE>();
	Collector<KEY, VALUE>  pivots = new Collector<KEY, VALUE>();
	Collector<KEY, VALUE> finalResult = new Collector<KEY, VALUE>();
	Mapper<KEY, VALUE> mapper = null;
	Reducer<KEY, VALUE> reducer = null;
	static int p ;

	public Node(Mapper<KEY, VALUE> mapper, Reducer<KEY, VALUE> reducer) {
		super();
		this.mapper = mapper;
		this.reducer = reducer;
	}

	
	/**
	 * For details read inline commnets 
	 * 
	 * @param args
	 *		  1. iplist
	 *		  2. s3 input path
	 *		  3. s3 output path
	 * 		  
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException 
	 */
	public void run() {
		try {
			
			// read ip files , put into map
			App.s3  = new AmazonS3Client(new BasicAWSCredentials(App.key, App.pwd));
			AWSUtility.s3ReadIpFile(App.bucket, App.ipFile);

			// Start Client , connect to master to get file list, close client conn
			// App.masterip = "localhost";
			logger.info("master = "+ App.masterip);
			Socket client = new Socket(App.masterip, 7077);
			logger.info("Just connected to " + client.getRemoteSocketAddress());

			// get files to read from master 
			List<Object> outputList = new ArrayList<Object>();
			outputList.add("getFiles");
			outputList.add(App.nodeIdentity);
			ObjectOutputStream objectOut = new ObjectOutputStream(client.getOutputStream());
			objectOut.writeObject(outputList);
			ObjectInputStream in = new ObjectInputStream(client.getInputStream());
			List<String> filesList = (ArrayList<String>) in.readObject();
			logger.info(filesList);

			// Read File, Sort and Create pivots
			for (String fileName : filesList) {
				readS3FileContentsAndCallMapper(App.bucket, fileName);
			}
			localData.sort();
			p = App.ipmap.size();
			int w = localData.count()/p;
			for (int j=0; j < p; j++) {
				pivots.collect(localData.toList().get(j*w));
			}
			logger.info("pivots = "+ pivots);
			
			// Start Client , connect to master to send pivots
			Socket client2 = new Socket(App.masterip, 7077);
			outputList = new ArrayList<Object>(); 
			outputList.add("pivot"); 
			outputList.add(pivots);
			objectOut = new ObjectOutputStream(client2.getOutputStream());
			objectOut.writeObject(outputList);

			// request global pivots
			Collector<KEY, VALUE> globalPivots = null;
			Socket client3 = new Socket(App.masterip, 7077);
			outputList = new ArrayList<Object>(); 
			outputList.add("globalpivots"); 
			objectOut = new ObjectOutputStream(client3.getOutputStream());
			objectOut.writeObject(outputList);
			in = new ObjectInputStream(client3.getInputStream());
			Object outObj = in.readObject();
			globalPivots = (Collector<KEY, VALUE>) outObj;
			logger.info("globalPivots = "+ globalPivots);


			// Split (Shuffle) based on global pivots  
			Map<Integer, Collector<KEY, VALUE>> nodalFiles = new HashMap<Integer, Collector<KEY, VALUE>>();
			nodalFiles = getSections(globalPivots);

			// Start client connection to other nodes, share data based on pivot
			for (Map.Entry<Integer, Collector<KEY, VALUE>> entry : nodalFiles.entrySet()) {
				Integer nodeNum = entry.getKey();
				createFolderAndWriteFileS3("output-temp"+nodeNum, "output-000"+App.nodeIdentity, entry.getValue());
			}
			
			// ready for reducer
			Socket client4 = new Socket(App.masterip, 7077);
			outputList = new ArrayList<Object>();
			outputList.add("dataready");
			objectOut = new ObjectOutputStream(client4.getOutputStream());
			objectOut.writeObject(outputList);
			s3ReadBucketsFolder();
			
			//call reduce method and write to disk
			Map<KEY, Collector<KEY, VALUE>> shuffle = finalResult.subCollectors();
			Set<KEY> keys = shuffle.keySet();
			Collector<KEY,VALUE> out = new Collector<KEY,VALUE>();
			for (KEY key : keys) {
				Collector<KEY,VALUE> s_source = shuffle.get(key);
				reducer.reduce(out, key, s_source);
			}
			
			// all tasks finished
			Socket client6 = new Socket(App.masterip, 7077);
			outputList = new ArrayList<Object>(); 
			outputList.add("finish");
			objectOut = new ObjectOutputStream(client6.getOutputStream());
			objectOut.writeObject(outputList);

			//close all sockets and streams
			in.close();
			objectOut.close();
			client.close();
			client2.close();
			client3.close();
			client4.close();
			client6.close();
			
			//write final files to output folder
			AWSUtility.finalWriteToS3(App.output, "output-000"+App.nodeIdentity, out);
		} catch (IOException | ClassNotFoundException e) {
			logger.error(e.getLocalizedMessage());
		}

	}

	
	/**
	 * Shuffle phase 
	 * Distributes files based on the key(node number here)
	 * 
	 * @param pivots
	 * @return map containing key and its section
	 */
	private Map<Integer, Collector<KEY, VALUE>> getSections(Collector<KEY, VALUE> pivots) {		
		Map<Integer, Collector<KEY, VALUE>> sections = new HashMap<Integer, Collector<KEY, VALUE>>();
		Integer currentPivotsIndex = 0;
		Integer from = 0;
		Integer key = 1;
		Tuple<KEY, VALUE> pivot = pivots.toList().get(currentPivotsIndex);

		for (Integer i = 0; i < localData.count(); i++) {
			if (localData.toList().get(i).compareTo(pivot) == 1) {
				Collector<KEY, VALUE> sublist = new Collector<>();
				sublist.addAll(localData.toList().subList(from, i));
				sections.put(key, sublist);
				from = i;
				currentPivotsIndex ++;
				key++;
				if (currentPivotsIndex >= pivots.count()) break;
				pivot = pivots.toList().get(currentPivotsIndex);
			}
		}
		Collector<KEY, VALUE> sublist = new Collector<>();
		sublist.addAll(localData.toList().subList(from, localData.count()));
		sections.put(key, sublist);
		return sections;
	}
	
	/**
	 * reads file from bucket and for each row, calls the mapper's map function
	 * Output is added to the localData 
	 * 
	 * @param bucket
	 * @param file : mapper file to read 
	 * @throws IOException
	 */
	private void readS3FileContentsAndCallMapper(String bucket, String file) throws IOException {
		S3Object s3o = App.s3.getObject(new GetObjectRequest(bucket,file));
		BufferedReader reader=new BufferedReader(new InputStreamReader(s3o.getObjectContent()));
		String line=null;
		Integer lineNum = 0;
		while((line=reader.readLine()) != null){
			Tuple<KEY, VALUE> record = new Tuple<>();
			record.setKey((KEY) lineNum++);
			record.setValue((VALUE) line);
			mapper.map(localData, record);
		}
		reader.close();
	}

	/**
	 * Read bucket and calls readFiles for each bucket
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public void s3ReadBucketsFolder() throws IOException, ClassNotFoundException {
		ObjectListing objs = App.s3.listObjects(new ListObjectsRequest().withBucketName(App.bucket).withPrefix("output-temp"+App.nodeIdentity));
		for (S3ObjectSummary obj : objs.getObjectSummaries()) {
			readFiles(obj.getKey());
		}
	}

	/**
	 * Read File (containnig the tuple object) 
	 * adds it to final result collector.
	 * 
	 * @param fileName
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private void readFiles(String fileName) throws IOException, ClassNotFoundException {
		S3Object s3o = App.s3.getObject(new GetObjectRequest(App.bucket, fileName));
		ObjectInputStream ois = new ObjectInputStream(s3o.getObjectContent());
		try{
			while (true) {
				Tuple<KEY, VALUE> tuple = (Tuple<KEY, VALUE>)ois.readObject();
				finalResult.collect(tuple);
			}
		} catch(EOFException ex){}
	}


	/**
	 * Writes data object  to file in corresponding folder 
	 * 
	 * @param folderS : folder to write files to 
	 * @param fileS : file name to write 
	 * @param data : data to be written
	 * @throws IOException
	 */
	private void createFolderAndWriteFileS3(String folderS, String fileS, Collector<KEY, VALUE> data) throws IOException{
		String folder = folderS+"/";
		String fileName=folder+fileS;
		File file=File.createTempFile(fileName,"");
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file));
		while (data.hasNext()) {
			Tuple<KEY,VALUE> t = data.next();
			oos.writeObject(t);
		}
		oos.close();
		App.s3.putObject(new PutObjectRequest(App.bucket, fileName, file));
	}

}