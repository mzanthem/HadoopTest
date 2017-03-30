/*
 * LostOrder.java Created On 2017年3月28日
 * Copyright(c) 2017 ODY Inc.
 * ALL Rights Reserved.
 */
package com.mazan.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mazan.util.HDFSUtil;
import com.mazan.util.TxtReadUtil;

/**
 * LostOrder
 * 查找丢失的订单号
 * 遍历/input/20170326/下的日志文件
 * 查找给定的单号列表
 * 
 * output: 单号--原因
 * @time: 下午3:33:37
 * @author mazan
 */
public class LostOrder {

	public static final Log logger = LogFactory.getLog(LostOrder.class);
	
	public static List<String> list;
	public static Set<String> set;
	
	static {
		list = TxtReadUtil.getTxt("/tmp/37.csv");
		set = new HashSet<>();
	}
	
	public static void main(String[] args) throws Exception {
//		System.out.println(list.size());
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		
		//输出文件夹存在--删除
		HDFSUtil.delete(otherArgs[otherArgs.length - 1]);
		
		Job job = Job.getInstance(conf, "lost order");
		job.setJarByClass(LostOrder.class);
		job.setMapperClass(LostOrderMapper.class);
//		job.setCombinerClass(LostOrderReducer.class);
		job.setReducerClass(LostOrderReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ObjectWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

	/**
	 * 遍历log日志
	 * @author user
	 *
	 */
	public static class LostOrderMapper extends Mapper<Object, Text, Text, ObjectWritable> {
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String log = value.toString();
			String orderId = findLog(log, list);
			//找到日志，写入
			if(StringUtils.isNotEmpty(orderId)) {
				set.add(orderId);
//				String reason = getReason(log);
				context.write(new Text(orderId), new ObjectWritable(log));
//				logger.info("find!!! orderId=" + orderId + "reason is " + log.substring(55));
//				System.out.println("find!!! orderId=" + orderId + "reason is " + log.substring(55));
			}
			
		}

		@Override
		protected void cleanup(Mapper<Object, Text, Text, ObjectWritable>.Context context)
				throws IOException, InterruptedException {
			// map clean up
			
			while(context.nextKeyValue()) {
				continue;
			}
			
			System.out.println("-----------------mapper clean up-----------------");
			
			
		}
		
		
		
		
	}
	
	/**
	 * 输出汇总结果
	 * @author user
	 *
	 */
	public static class LostOrderReducer extends Reducer<Text, ObjectWritable, Text, ObjectWritable> {
		
		@Override
		public void reduce(Text key, Iterable<ObjectWritable> values, Context context)
				throws IOException, InterruptedException {
			
//			System.out.println("Set.size = " + set.size());
			
			ObjectWritable result = new ObjectWritable();
			for (ObjectWritable val : values) {
				result = val;
			}
			//判断List没找到的元素方法1：list.remove
			
			
			context.write(key, result);
		}

		@Override
		protected void cleanup(Reducer<Text, ObjectWritable, Text, ObjectWritable>.Context context)
				throws IOException, InterruptedException {
			
			 while (context.nextKey()) {
				 continue;
			 }
			//reduce完成后的操作
			System.out.println("---------clean up--Set.size = " + set.size());
			
			List<String> remain = new ArrayList<>(list);
			remain.removeAll(set);
			System.out.println(remain.size());
			for(int i = 0; i< remain.size(); i++) {
				System.out.println("-------------remain:" + remain.get(i));
			}
		}
		
		
	}
	
	/**
	 * 是否有订单日志记录
	 * @param log
	 * @param list
	 * @return
	 */
	public static String findLog(String log, List<String> list) {
		
		for (String str : list) {
			if (log.indexOf(str) != -1) {
				return str;
			}
		}
		return "";
	}
	
	public static String getReason(String log) {
		
		List<String> keys = new ArrayList<>();
		keys.add("处理prs异常");
		keys.add("pl为空");
		keys.add("传入的int参数 os 是 为空");
		keys.add("sdk check mem中app状态1");
		
		for (String key : keys) {
			String result = getReason(key);
			if (StringUtils.isNotEmpty(result)) return result;
		}
		return "unkown";
		
	}
	
	
}

