package com.assignment.a7;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import quickml.data.instances.ClassifierInstance;
import quickml.supervised.ensembles.randomForest.randomDecisionForest.RandomDecisionForest;
import quickml.supervised.ensembles.randomForest.randomDecisionForest.RandomDecisionForestBuilder;
import quickml.supervised.tree.decisionTree.DecisionTreeBuilder;

import com.opencsv.CSVParser;

/**
 * @author Shakti Patro
 * @author Pankaj Tripathi
 * @author Kartik Mahaley
 * @author Chen Bai
 * 
 * Functionality :
 * 1. Reads in connections created from history files  
 * 2. Creates models for each (month,origin) or (origin,destination) pair
 * 3. Writes models to a temporary path on s3.
 */

public class ModelJob extends Configured implements Tool{

	static String SEPATRATOR = ",";
	static CSVParser csv_parser = new CSVParser();

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "ModelJob");
		job.setJarByClass(App.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(ModelMapper.class);
		job.setReducerClass(ModelReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Input : connections from intermediate path
	 * Output : key : month,carrier value : conection 
	 * Note:
	 * 		[2],[5] for (month,origin) as key, 
	 * 		[5],[7] for (origin,destination) as key 
	 */
	public static class ModelMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = csv_parser.parseLine(value.toString());
			context.write(new Text(line[2]+SEPATRATOR+line[5]), value); 	
		}
	}

	/**
	 * Input: all connections for each (month,origin)
	 * Output :models for each key 
	 * Each model is created using the RandomForest function of quickML
	 * models are seralized into the /serializedObjects folder
	 *
	 */
	public static class ModelReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			List<ClassifierInstance> dataset = Utils.loadDataSet(values);
			if(dataset != null) {
				RandomDecisionForest randomForest = new RandomDecisionForestBuilder<ClassifierInstance>(
						new DecisionTreeBuilder<ClassifierInstance>()).buildPredictiveModel(dataset);
				Utils.serializeObject(key, randomForest, context.getConfiguration());	
			}
		}
	}
}