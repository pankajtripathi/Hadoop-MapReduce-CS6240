package com.assignment.a7;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import quickml.data.AttributesMap;
import quickml.supervised.ensembles.randomForest.randomDecisionForest.RandomDecisionForest;

import com.opencsv.CSVParser;

/**
 * @author Shakti Patro
 * @author Pankaj Tripathi
 * @author Kartik Mahaley
 * @author Chen Bai
 * 
 * Functionality :
 * 1. This class takes in request files and test connections 
 * 2. Predicts missed flights for the requested (origin-destination) connections  
 * 3. Uses the models stored in /serializedObjects for making predictions
 */
public class PredictionJob extends Configured implements Tool{

	static String SEPARATOR = ",";
	static CSVParser csv_parser = new CSVParser();

	/**
	 * 1. Takes multiple inputs viz: "test connections" & "request" 
	 * 2. Calls mappers based on input and waits till execution completes 
	 *
	 */
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "PredictionJob");
		job.setJarByClass(App.class);
		job.setMapperClass(ConnectionMapper.class);
		job.setMapperClass(RequestMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,ConnectionMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class,RequestMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setReducerClass(PredictionReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Input: test connections
	 * Output: 
	 * 		Key 	: Year, Month, Day, Origin, Destination
	 * 		Value	: "C" + connectionData   
	 *
	 */
	public static class ConnectionMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = csv_parser.parseLine(value.toString());
			if(line.length < 7 ) {
				System.out.println(value.toString());
			}
			String outputKey=line[1] +SEPARATOR+ line[2] +SEPARATOR+ line[3] +SEPARATOR+ 
					line[5] +SEPARATOR+ line[7];
			context.write(new Text(outputKey), new Text("C"+SEPARATOR+value.toString()));
		}
	}

	/**
	 * Input: Requests 
	 * Output: 
	 * 		Key 	: Year, Month, Day, Origin, Destination
	 * 		Value	: "R" + connectionData   
	 *
	 */
	public static class RequestMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = csv_parser.parseLine(value.toString());
			String outputKey=line[0] +SEPARATOR+ line[1] +SEPARATOR+ line[2] +SEPARATOR+ 
					line[3] +SEPARATOR+ line[4];
			context.write(new Text(outputKey),  new Text("R"+SEPARATOR+value.toString()));
		}
	}

	/**
	 * Input: 
	 * 		Key 	: Year, Month, Day, Origin, Destination
	 * 		Value	: "C" + connectionData   
	 * Output:
	 * 		1. Checks if there is a request for this key 
	 * 		2. Checks if any connection exist for this key
	 * 		3. Checks if a model exists for this key and calls prediction on that model
	 * 		4. If model is not present, then just predict that all connections are not missed 
	 * 		5. Returns data with duration and predicted missed 
	 */
	public static class PredictionReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String request = null;
			List<String[]> conns = new ArrayList<String[]>();
			for (Text value : values) {
				if(csv_parser.parseLine(value.toString())[0].equals("R")) request = value.toString();
				else conns.add(csv_parser.parseLine(value.toString()));
			}
			if(StringUtils.isEmpty(request) || CollectionUtils.isEmpty(conns)) return;
			int minDuration = Integer.MAX_VALUE;
			String[] keys = key.toString().split(SEPARATOR); 
			String[] finalValue = null;
			if (Utils.forestExists(keys[1]+"_"+keys[3], context.getConfiguration())) {
				RandomDecisionForest randomForest = Utils.deserializeObject(keys[1]+"_"+keys[3], context.getConfiguration());
				AttributesMap attributes;
				for (String[] line : conns) {
					attributes = new AttributesMap();
					attributes = AttributesMap.newHashMap();
					attributes.put("carrier", line[1]);
					attributes.put("month", line[3]);
					attributes.put("day_of_month", line[4]);
					attributes.put("day_of_week", line[5]);
					attributes.put("origin", line[6]);
					attributes.put("dest", line[7]);
					attributes.put("arr_time", line[11]);
					attributes.put("elapsed_time", line[12]);
					attributes.put("dist_group", line[13]);
					attributes.put("missed", "?");
					String isMissed = randomForest.getClassificationByMaxProb(attributes).toString();
					int duration = Integer.parseInt(line[14]);
					if(Boolean.getBoolean(isMissed)) {
						duration += 100;
					}
					if(duration < minDuration) {
						finalValue = line;
						finalValue[14] = String.valueOf(duration);
					}
				}
			} else {
				for (String[] line : conns) {
					int duration = Integer.parseInt(line[14]);
					if(duration < minDuration) {
						finalValue = line;
						finalValue[14] = String.valueOf(duration); 
					}
				}
			}
			context.write(key, new Text(finalValue[9]+SEPARATOR+ finalValue[10]+SEPARATOR+finalValue[14]));
		}
	}
}