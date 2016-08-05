package com.bigdata.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MedalsByEachCountryMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	Text country = new Text();
	private final static IntWritable one = new IntWritable(1);
	private String SWIMMING = "Swimming";
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String record = value.toString();
		String recordValuesArr[] = record.split("\t");
		if(recordValuesArr.length==10){
			String sport = recordValuesArr[5];
			if(SWIMMING.equals(sport)){
				String countryStr = recordValuesArr[2];
				country.set(countryStr);
				context.write(country, one);
			}
		}
	}
}
