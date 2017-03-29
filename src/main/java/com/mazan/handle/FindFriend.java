/*
 * FindFriend.java Created On 2017年3月29日
 * Copyright(c) 2017 Mazan Inc.
 * ALL Rights Reserved.
 */
package com.mazan.handle;

import java.util.ArrayList;
import java.util.List;

/**
 * FindFriend
 *
 * @time: 下午11:24:45
 * @author mazan
 */
public class FindFriend {

	public static void main(String[] args) {
		List<String> list = init();
		 
		for (String str : list) {
			mapper(str);
		}
		
		
		String d = "D";
		String f = "F";
		System.out.println(d.compareTo(f));
	}

	/**
	 * 模拟mapper
	 * @param str
	 */
	private static void mapper(String str) {
		// TODO Auto-generated method stub
		
	}
	
	private static void reducer() {
		
	}
	

	public static List<String> init() {
		List<String> list = new ArrayList<>();
		list.add("A B C D E F");
		list.add("B A C D E");
		list.add("C A B E");
		list.add("D A B E");
		list.add("E A B C D");
		list.add("F A");
		return list;
	}
}

