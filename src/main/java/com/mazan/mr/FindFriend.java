/*
 * FindFriend.java Created On 2017年3月29日
 * Copyright(c) 2017 Mazan Inc.
 * ALL Rights Reserved.
 */
package com.mazan.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

import com.mazan.util.HDFSUtil;

/**
 * FindFriend
 *
 * @time: 下午11:01:35
 * @author mazan
 */
public class FindFriend {

	public static Map<String, Set<String>> map;
	
	static {
		
		map = new HashMap<>();
	}
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
	 * A&B C,D,E
	 * E&F A
	 *  
	 */
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}

		//输出文件夹存在--删除
		HDFSUtil.delete(otherArgs[otherArgs.length - 1]);
		
		Job job = Job.getInstance(conf, "find friend");
		job.setJarByClass(FindFriend.class);
		job.setMapperClass(FindFriendMapper.class);
		job.setReducerClass(FindFriendReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		for (int i = 0; i < otherArgs.length - 1; i++) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	public static class FindFriendMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] friends = line.split(" ");
			
			System.out.println("line :" + line);
			
			//共同的朋友：owner
			String owner = friends[0];
			for (int i = 1; i < friends.length; i++) {
				//朋友
				String friend = friends[i];
				//得到拥有该朋友的owner
				Set<String> others = map.get(friend);
				if (null == others ||others.isEmpty()) {
					others = new HashSet<String>();
				}
				others.add(owner);
				map.put(friend, others);
			}
			//比较当前行
			//处理others,输出
			
			for(Map.Entry<String, Set<String>> entry : map.entrySet()) {
				String ek = entry.getKey();
				Set<String> ev = entry.getValue();
				//组合ev
				List<String> list = new ArrayList<>(ev);
				if (list.size() > 1) {
					for (int i = 0; i < list.size(); i++) {
						for (int j = i + 1; j < list.size(); j++) {
							String resultKey = getAbs(list.get(i), list.get(j));
							//输出["A&B", C]
							context.write(new Text(resultKey), new Text(ek));
						}
					}
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

	public static class FindFriendReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			Set<String> result = new HashSet<String>();
			for (Text value : values) {
				result.add(value.toString());
			}
			
			context.write(new Text(key), new Text(result.toString()));
			
		}
	}
}
