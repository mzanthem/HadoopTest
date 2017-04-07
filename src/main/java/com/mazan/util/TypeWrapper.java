package com.mazan.util;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

public class TypeWrapper {
	public static Text wrapString(String value){
		return new Text(value);
	}
	public static Text wrapString(MapWritable map,String key){
		return (Text)(map.get(wrapString(key)));
	}
	
	public static LongWritable wrapLong(MapWritable map,String key){
		return wrapLong((Text)map.get(wrapString(key)));
	}
	public static LongWritable wrapLong(Long value){
		return new LongWritable(value);
	}
	
	public static LongWritable wrapLong(String value){
		long val = 0;
		try{
			val = Long.parseLong(value.toString());
		}catch(Exception e){
			// Nothing to do.
		}
		return wrapLong(val);
	}
	public static LongWritable wrapLong(Text value){
		if(value == null ){
			value = wrapString("0");
		}
		return wrapLong(value.toString());
	}
	
	public static FloatWritable wrapFloat(Float value){
		return new FloatWritable(value);
	}
	
	public static FloatWritable wrapFloat(String value){
		float val = 0;
		try{
			val = Float.parseFloat(value.toString());
		}catch(Exception e){
			// Nothing to do.
		}
		return new FloatWritable(val);
	}
	public static FloatWritable wrapFloat(MapWritable map,String key){
		return wrapFloat((Text)map.get(wrapString(key)));
	}
	public static FloatWritable wrapFloat(Text value){
		if(value == null ){
			value = wrapString("0");
		}
		float val = 0;
		try{
			val = Float.parseFloat(value.toString());
		}catch(Exception e){
			// Nothing to do.
		}
		return wrapFloat(val);
	}
	public static IntWritable wrapInteger(MapWritable map,String key){
		return wrapInteger((Text)map.get(wrapString(key)));
	}
	
	public static IntWritable wrapInteger(String value){
		int val = 0;
		try{
			val = Integer.parseInt(value.toString());
		}catch(Exception e){
			// Nothing to do.
		}
		return wrapInteger(val);
	}
	public static IntWritable wrapInteger(Text value){
		if(value == null ){
			value = wrapString("0");
		}
		int val = 0;
		try{
			val = Integer.parseInt(value.toString());
		}catch(Exception e){
			// Nothing to do.
		}
		return wrapInteger(val);
	}
	public static IntWritable wrapInteger(Integer value){
		return new IntWritable(value);
	}
	
	public static MapWritable wrapMap(MapWritable map,String key){
		return (MapWritable)map.get(wrapString(key));
	}
	
}
