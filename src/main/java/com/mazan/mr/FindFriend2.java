/*
 * FindFriend2.java Created On 2017年3月30日
 * Copyright(c) 2017 ODY Inc.
 * ALL Rights Reserved.
 */
package com.mazan.mr;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mazan.util.HDFSUtil;

/**
 * FindFriend2
 * 参考答案--前提：给出的内容是幂等
 * 如果只给出
 * A B C D E F
 * B A C D E
 * 则得不到想要的结果
 * @time: 上午9:48:34
 * @author mazan
 */
public class FindFriend2 {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}

		//输出文件夹存在--删除
		HDFSUtil.delete(otherArgs[otherArgs.length - 1]);
        
		Job job = Job.getInstance(conf, "find friend2");
		job.setJarByClass(FindFriend2.class);
		job.setMapperClass(FindFriend2Mapper.class);
		job.setReducerClass(FindFriend2Reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		for (int i = 0; i < otherArgs.length - 1; i++) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	/**
	 * 另外一种理解  A 是xxx共同的朋友？-- B、C、D、E、F!
	 * @author user
	 *
	 */
	public static class FindFriend2Mapper extends Mapper<Object, Text, Text, Text> {
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString());
			
			Text owner = new Text();
			owner.set(itr.nextToken()); //第一个是拥有者
			
			Set<String> set = new HashSet<>();
			while (itr.hasMoreTokens()) {
				set.add(itr.nextToken());
			}
			
			String[] friends = new String[set.size()];
			friends = set.toArray(friends);
			
			for (int i = 0; i < friends.length; i++) {
				for (int j = i + 1; j < friends.length; j++) {
					String outputKey = getAbs(friends[i], friends[j]); //即时无序也变成有序
					context.write(new Text(outputKey), owner);
				}
			}
			
		}
		
		private static String getAbs(String str0, String str1) {
			//str0<str1
			if (str0.compareTo(str1) < 0) {
				return str0 + "&" + str1;
			}
			
			return str1 + "&" + str0;
		}
		
	}
	
	
	public static class FindFriend2Reducer extends Reducer<Text, Text, Text, Text> {
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			//使用set去重
			Set<String> result = new HashSet<String>();
			
			for (Text value : values) {
				result.add(value.toString());
			}
			
			context.write(key, new Text(result.toString()));
			
		}
	}
}

