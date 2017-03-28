package com.mazan.mr;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * sample.txt
 * 123456798676231190101234567986762311901012345679867623119010123456798676231190101234561+00121534567890356
 * 123456798676231190101234567986762311901012345679867623119010123456798676231190101234562+01122934567890456
 * 123456798676231190201234567986762311901012345679867623119010123456798676231190101234562+02120234567893456
 * 123456798676231190401234567986762311901012345679867623119010123456798676231190101234561+00321234567803456
 * 123456798676231190101234567986762311902012345679867623119010123456798676231190101234561+00429234567903456
 * 123456798676231190501234567986762311902012345679867623119010123456798676231190101234561+01021134568903456
 * 123456798676231190201234567986762311902012345679867623119010123456798676231190101234561+01124234578903456
 * 123456798676231190301234567986762311905012345679867623119010123456798676231190101234561+04121234678903456
 * 123456798676231190301234567986762311905012345679867623119010123456798676231190101234561+00821235678903456
 */
public class MaxTemperature {

	public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static final int MISSING = 9999;

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String year = line.substring(15, 19);
			
			int airTemperature;
			if (line.charAt(87) == '+') {
				airTemperature = Integer.parseInt(line.substring(88, 92));
			} else {
				airTemperature = Integer.parseInt(line.substring(87, 92));
			}
			
			String quality = line.substring(92, 93);
			
			if (airTemperature != MISSING && quality.matches("[01459]")) {
				context.write(new Text(year), new IntWritable(airTemperature));
			}
		
		}
		
	}
	
	public static class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int maxValue = Integer.MIN_VALUE;
			for (IntWritable value : values) {
				maxValue = Math.max(maxValue, value.get());
			}
			context.write(key, new IntWritable(maxValue));
		}
		
	}
	
	/**
	 * 
	 * @param args
	 * 	hdfs://hadoop:9000/input/sample.txt
	 *	hdfs://hadoop:9000/output/20170315/maxtemperature
	 * @throws Exception
	 */
	public static void main (String[] args) throws Exception {
		
		if (args.length < 2) {
			System.err.println("Usage: MaxTemperature <input path> <output path>");
			System.exit(-1);
		}
		
		Job job = Job.getInstance();
		job.setJarByClass(MaxTemperature.class);
		job.setJobName("Max temperature");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setCombinerClass(MaxTemperatureReducer.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	
}
