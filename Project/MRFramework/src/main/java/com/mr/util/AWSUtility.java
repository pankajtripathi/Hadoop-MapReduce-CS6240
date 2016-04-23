package com.mr.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.mr.io.Collector;
import com.mr.io.Tuple;
import com.mr.net.App;

/**
 * UTILITY class for all AWS read and write functions
 * This contains only those classes which can be used by multiple methods
 * Other methods might be present in individual classes in other packages
 *  
 * @author Shakti
 *
 */
public class AWSUtility {

	
	/**
	 * Reads the ipaddress.txt file and updates the ipmap in App class
	 * 
	 * @param bucket
	 * @param file
	 * @throws IOException
	 */
	public static void s3ReadIpFile(String bucket, String file) throws IOException {
		S3Object s3o = App.s3.getObject(new GetObjectRequest(bucket,file));
		BufferedReader reader=new BufferedReader(new InputStreamReader(s3o.getObjectContent()));
		while(true){
			String line=reader.readLine();
			if(line==null)break;
			String[] ips  = line.split(","); 
			int nodeNum = Integer.parseInt(ips[0]);
			if(nodeNum ==0) App.masterip = ips[2];
			else App.ipmap.put(nodeNum, line);
		}
	}
	
	
	/**
	 * Reads the input folder in S3 and creates file names list for each node
	 * 
	 * @param bucket : S3 bucket name 
	 * @param folder : folder from where files are to be read
	 * @returns List of file names for each node
	 *  
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static List<String>[] s3ReadBuckets(String bucket, String folder) throws IOException {
		ObjectListing objs = App.s3.listObjects(new ListObjectsRequest().withBucketName(bucket).withPrefix(folder));
		Integer fileCount = 0;
		int nodes = App.ipmap.size();
		System.out.println("nodes = "+ nodes);
		List<String>[] filesForNodes = new ArrayList[nodes]; 
		for (S3ObjectSummary obj : objs.getObjectSummaries()) {
			List<String> files = filesForNodes[fileCount%nodes];
			if(files == null) files = new ArrayList<String>();
			files.add(obj.getKey());
			filesForNodes[fileCount%nodes] = files;
			fileCount++;
		}
		return filesForNodes;
	}
	
	
	/**
	 * Writes final output to s3 in the output folder specified
	 * We have used tab as separator between key and value
	 * 
	 * @param folderS
	 * @param fileS
	 * @param data
	 * @throws IOException
	 */
	public static void finalWriteToS3(String folderS, String fileS, Collector<?,?> data) throws IOException{
		String folder = folderS+"/";
		String fileName=folder+fileS;
		File file=File.createTempFile(fileName,"");
		Writer writer = new OutputStreamWriter(new FileOutputStream(file));
		while (data.hasNext())
		{
			Tuple<?,?> t = data.next();
			writer.write(t.getKey() + "\t"+  t.getValue()+"\n");
		}
		writer.close();
		App.s3.putObject(new PutObjectRequest(App.bucket, fileName, file));
	}
	
}
