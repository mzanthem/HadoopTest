/*
 * TxtReadUtil.java Created On 2017年3月28日
 * Copyright(c) 2017 ODY Inc.
 * ALL Rights Reserved.
 */
package com.mazan.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

/**
 * TxtReadUtil
 *
 * @time: 下午3:36:39
 * @author mazan
 */
public class TxtReadUtil {

	/**
	 * 读取txt
	 * 
	 * @param filePath
	 * @return
	 */
	public static List<String> getTxt(String filePath) {

		if (StringUtils.isEmpty(filePath)) {
			return null;
		}

		List<String> list = new ArrayList<>();
		try {
			File file = new File(filePath);

			if (file.isFile() && file.exists()) {
				// 考虑到编码格式
				InputStreamReader read = new InputStreamReader(new FileInputStream(file), "GBK");
				BufferedReader bufferedReader = new BufferedReader(read);
				String lineTxt = null;
				while ((lineTxt = bufferedReader.readLine()) != null) {
					System.out.println(lineTxt);
					list.add(lineTxt);
				}
				read.close();
			} else {
				System.out.println("找不到指定的文件");
			}
		} catch (Exception e) {
			System.out.println("读取文件内容出错");
			e.printStackTrace();
		}

		return list;
	}
}
