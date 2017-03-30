/*
 * HDFSUtil.java Created On 2017年3月30日
 * Copyright(c) 2017 ODY Inc.
 * ALL Rights Reserved.
 */
package com.mazan.util;

import java.io.File;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * HDFSUtil
 *
 * @time: 下午1:34:26
 * @author mazan
 */
public class HDFSUtil {

	/**
	 * defaultURI
	 */
	public static final String uri = "hdfs://heimdall01.test.hadoop.com:8020/";

	/**
	 * 获得默认FileSystem
	 * 
	 * @return
	 * @throws Exception
	 */
	public static FileSystem getDefaultFs() throws Exception {

		// 读取classpath下的xxx-site.xml 配置文件，并解析其内容，封装到conf对象中
		Configuration conf = new Configuration();
		// 也可以在代码中对conf中的配置信息进行手动设置，会覆盖掉配置文件中的读取的值
		conf.set("fs.defaultFS", uri);

		return getFs(conf);
	}

	/**
	 * 获得默认FileSystem
	 * 
	 * @return
	 * @throws Exception
	 */
	public static FileSystem getFs(Configuration conf) throws Exception {
		FileSystem fs = FileSystem.get(new URI(uri), conf, "user");
		return fs;
	}

	/**
	 * 本地文件上传
	 * 
	 * @param source
	 *            本地文件位置
	 * @param target
	 *            hdfs文件位置
	 * @throws Exception
	 */
	public static void upload(String source, String target) throws Exception {
		final FileSystem fileSystem = getDefaultFs();
		fileSystem.copyFromLocalFile(new Path(source), new Path(target));

	}

	/**
	 * 上传文件夹内容
	 * 
	 * @param source
	 *            源文件夹
	 * @param target
	 *            hdfs文件夹
	 * @throws Exception
	 */
	public static void uploadFolder(String source, String target) throws Exception {
	
		final FileSystem fileSystem = getDefaultFs();
	
		File folder = new File(source);
		if (folder.isDirectory()) {
	
			for (File file : folder.listFiles()) {
				fileSystem.copyFromLocalFile(new Path(file.getPath()), new Path(target + file.getName()));
			}
	
		}
	}

	/**
	 * HDFS文件下载
	 * 
	 * @param source
	 *            hdfs文件位置
	 * @param target
	 *            本地文件位置
	 * @throws Exception
	 */
	public static void download(String source, String target) throws Exception {
		final FileSystem fileSystem = getDefaultFs();
		fileSystem.copyToLocalFile(new Path(source), new Path(target));
	}

	/**
	 * 下载文件夹内容
	 * 
	 * @param source
	 *            hdfs文件夹
	 * @param target
	 *            源文件夹
	 * @throws Exception
	 */
	public static void downloadFolder(String source, String target) throws Exception {

		final FileSystem fileSystem = getDefaultFs();
		// 不需要递归遍历
		FileStatus[] listStatus = fileSystem.listStatus(new Path(source));
		// 空文件夹
		if (null == listStatus || listStatus.length == 0) {
			return;
		}

		// 创建本地文件夹
		File folder = new File(source);
		if (!folder.exists()) {
			folder.mkdirs();
		}

		for (FileStatus status : listStatus) {

			// 是文件，则下载
			if (status.isFile()) {
				Path path = status.getPath();
				String name = path.getName();
				System.out.println(name + (status.isDirectory() ? " is dir" : " is file"));
				fileSystem.copyToLocalFile(path, new Path(target + File.pathSeparator + name));
			}

		}

	}

	/**
	 * 删除文件
	 * 
	 * @param path
	 * @throws Exception
	 */
	public static void delete(String path) throws Exception {
		final FileSystem fileSystem = getDefaultFs();
		if (fileSystem.exists(new Path(path))) {
			fileSystem.delete(new Path(path), true);
		}
	}

}
