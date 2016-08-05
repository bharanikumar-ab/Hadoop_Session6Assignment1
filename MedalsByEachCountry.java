package com.bigdata.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.bigdata.mapper.MedalsByEachCountryMapper;
import com.bigdata.reducer.MedalsByEachCountryReducer;

public class MedalsByEachCountry {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration config = new Configuration();
		Job job = new Job(config,"MedalsByCountry");
		job.setJarByClass(MedalsByEachCountry.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(MedalsByEachCountryMapper.class);
		job.setCombinerClass(MedalsByEachCountryReducer.class);
		job.setReducerClass(MedalsByEachCountryReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new
				Path(args[1]));
		job.waitForCompletion(true);
	}
}
