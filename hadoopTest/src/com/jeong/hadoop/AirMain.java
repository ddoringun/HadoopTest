package com.jeong.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AirMain extends Configured implements Tool{

	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new AirMain(), args);
		System.exit(exitCode);
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		Job conf = Job.getInstance(getConf(), "AirMain");
		
		//출력 형식 지정
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		
		//Map 출력형식 지정
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		
		//Map Reduce
		conf.setMapperClass(Map01.class);
		conf.setReducerClass(Reduce01.class);
		
		//입출력 형식 지정
		conf.setInputFormatClass(TextInputFormat.class);
		conf.setOutputFormatClass(TextOutputFormat.class);
		
		//입출력 경로 지정
		FileInputFormat.setInputPaths(conf, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(conf,new Path(arg0[1]));
		
		//실행
		conf.waitForCompletion(true);
		
		return 0;
	}

}
