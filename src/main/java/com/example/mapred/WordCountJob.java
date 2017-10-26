package com.example.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


public class WordCountJob {

	//분산환경이기 때문에 jar 로 묶어서 컴파일해야 한다. ==> mvn package
	// hadoop com.example.mapred.WordCountJob ~/world/city.txt ~/world/out
	public static void main(String[] args) throws IOException {
		
		if (args.length != 2) {
			System.out.println("Usage : WordCountJob input output");
			System.exit(-2);
		}
		
		/*
		 * hadoop1 method
		 */
		JobConf job = new JobConf();
		
		//job name 설정
		job.setJobName("MyWordCount");
		
		job.setJarByClass(WordCountJob.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		JobClient.runJob(job);
		
		
	}

}
