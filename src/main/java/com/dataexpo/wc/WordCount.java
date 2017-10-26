package com.dataexpo.wc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class WordCount {

	public static void main(String[] args) throws IOException {

		JobConf job = new JobConf();
		
		job.setJobName("DataExpo Word Count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path inDir = new Path("/home/java/dataexpo/2008_nohead.csv");
		Path outDir = new Path("/home/java/dataexpo_2008_out");
		
		FileSystem hdfs = FileSystem.get(new Configuration());
		if (hdfs.exists(outDir)) {
			hdfs.delete(outDir, true);
		}
		
		FileInputFormat.setInputPaths(job, inDir);
		FileOutputFormat.setOutputPath(job, outDir);
		
		//jobTracker가 받아서 지시한다.
		JobClient.runJob(job);
	}

}
