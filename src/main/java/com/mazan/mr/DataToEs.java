/*
 * DataToEs.java Created On 2017年3月31日
 * Copyright(c) 2017 ODY Inc.
 * ALL Rights Reserved.
 */
package com.mazan.mr;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

import com.mazan.util.TypeWrapper;


/**
 * DataToEs 把HDFS上的数据发到ES HDFS: 
1#mazan#27#java
2#linhebin#27#php
3#baozhengxing#30#hadoop
4#majun#30#elasticsearch
5#huangbinbin#25#angularjs
 * @time: 上午11:11:54
 * @author mazan
 */
public class DataToEs {

	/*
	 * hdfs://heimdall01.test.hadoop.com/input/user.txt
	 * 172.16.0.205
	 * user/test
	 */
	public static void main(String[] args) throws Exception {
		
		if (args.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		
		//获取指定目录下的所有子一级目录
		final String hdfsPath = args[0]; //"hdfs://heimdall01.test.hadoop.com:8020/input/user/all/";
		final String esNodes = args[1];	//172.16.0.205:9200
		final String esIndex = args[2]; //user/test
		
		Configuration conf = new Configuration();
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);    
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false); 
		
		conf.set(ConfigurationOptions.ES_NODES, esNodes); //es.nodes
		conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");
		
		conf.set(ConfigurationOptions.ES_RESOURCE, esIndex);//index/type 从参数中读取
//		conf.set(ConfigurationOptions.ES_RESOURCE, "user/test"); //read Target index/type
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(DataToEs.class);
		job.setOutputFormatClass(EsOutputFormat.class);
		job.setMapperClass(DataToEsMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(MapWritable.class); 
		
		job.setSpeculativeExecution(false);//Disable speculative execution
		
		FileInputFormat.addInputPath(job, new Path(hdfsPath));
		job.waitForCompletion(true);
	

	}

	public static class DataToEsMapper extends Mapper<LongWritable, Text, NullWritable, MapWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] data = StringUtils.splitPreserveAllTokens(line, "#");
			String id = data[0];
			String name = data[1];
			String age = data[2];
			String remark = data[3];
			
			MapWritable doc = new MapWritable();
			doc.put(TypeWrapper.wrapString("id"), TypeWrapper.wrapString(id));
			doc.put(TypeWrapper.wrapString("name"), TypeWrapper.wrapString(name));
			doc.put(TypeWrapper.wrapString("age"), TypeWrapper.wrapString(age));
			doc.put(TypeWrapper.wrapString("remark"), TypeWrapper.wrapString(remark));
			
			context.write(NullWritable.get(), doc);

		}
	}

	// 没有reducer

}
