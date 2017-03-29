/*
 * MutualFriends.java Created On 2017年3月29日
 * Copyright(c) 2017 ODY Inc.
 * ALL Rights Reserved.
 */
package com.mazan.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * MutualFriends
 * 
 * @time: 下午5:53:04
 * @author mazan
 */
public class MutualFriend {
	/*
	 * 第一字母表示本人，其他是他的朋友，找出有共同朋友的人，和共同朋友是谁。（也可以采用伪代码）
	 * -------------
	 * A B C D E F
	 * B A C D E
	 * C A B E
	 * D A B E
	 * E A B C D
	 * F A
	 * -------------
	 * 
	 * 输出：
	 * A&B:C,D,E
	 *  
	 */
	/**
	 * 存放全局结果{"A&B","A&C"}
	 */
	public static Set<String> globeSet;
	
	static {
		globeSet = new HashSet<>();
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "mutual friend");
		job.setJarByClass(MutualFriend.class);
		job.setMapperClass(MutualFriendMapper.class);
		job.setReducerClass(MutualFriendReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		for (int i = 0; i < otherArgs.length - 1; i++) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}
	
	
	public static class MutualFriendMapper extends Mapper<Object, Text, Text, Text> {

		
		/**
		 * 存在之前的key就行
		 */
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] friends = line.split(" ");
			
			
			List<String> list;
			for (int i = 0; i < friends.length; i++) {
				
				for (int j = i + 1; j < friends.length; j++) {
					//每次均初始化list
					list = new ArrayList<>();
					list.addAll(Arrays.asList(friends));
					
					String index0 = friends[i];
					String index1 = friends[j];
					list.remove(index0);
					list.remove(index1);
					//比较key,获得“绝对值”
					String index =  getAbsEqual(index0 + "&" + index1);;
					//获得value
					Set<String> set;
					set = new HashSet<>();
					set.addAll(list);
					
					//输出
					context.write(new Text(index), new Text(set.toString()));
				}
			}
			
			
		}
		
		
		
	}

	
	public static class MutualFriendReducer extends Reducer<Text, Text, Text, Text> {
		/**
		 * 只有一个reducer
		 * @param key "A&B"
		 * @param values
		 */
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			List<Set<String>> list = new ArrayList<>();
			Set<String> result;
			
			int count = 0;
			//同一个key比较，values交集
			for (Text value : values) {
				String tmp = value.toString();
				System.out.println("=================" + tmp);
				
				
				result =  getSet(tmp);
				
				//至少有一个共同朋友
				if (result.size() > 1) {
					count++;
					list.add(result);
				}
				
			}
			
			if (count > 1) {
				
				Set<String> r1 = list.get(0);
				for (Set<String> set : list) {
					r1.retainAll(set);
				}
				//输出结果
				context.write(new Text(key),new Text(r1.toString()));
			}
			
		}
		
	}
	
	
	
	/**
	 * 判断2个字符串是否"绝对值"相等
	 * "AB" == "AB"
	 * "AB" == "BA"
	 * @return
	 */
	public static boolean isAbsEqual(String str0, String str1) {
		
		if (str0.equals(str1)) {
			return true;
		}
		
		if (str0.equals(reverseString(str1))) {
			return true;
		}
		
		return false;
	}
	
	
	public static String reverseString(String str) {
		StringBuffer stringBuffer = new StringBuffer(str);
		return stringBuffer.reverse().toString();
	}
	
	public static String getAbsEqual(String key) {
		if (globeSet.contains(key)) {
			return key;
		}
		if (globeSet.contains(reverseString(key))) {
			return reverseString(key);
		}
		//不包含，加入全局set
		globeSet.add(key);
		return key;
	}
	
	public static Set<String> getSet(String tmp) {
		
		if (tmp.startsWith("[")) {
			tmp = tmp.substring(1);
		}
		
		if (tmp.endsWith("]")) {
			tmp = tmp.substring(0, tmp.length() - 1);
		}
		
		String[] tmps = tmp.split(",");
		return new HashSet<String>(Arrays.asList(tmps));
		
	}
	
}

