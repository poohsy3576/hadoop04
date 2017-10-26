package com.world.wc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		/*
		 * HADOOP2 METHOD ==> org.apache.hadoop.mapreduce
		 */
		Configuration conf = new Configuration();
		
		Job job = new Job(conf);
		
		job.setJobName("World Word Count");
		
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		//출력 key, value type 정해준다.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileSystem hdfs = FileSystem.get(conf);
		
		Path inDir = new Path("/home/java/world/city.csv");
		Path outDir = new Path("/home/java/world_city");
		
		// default로 overwrite가 안되도록 되있기 때문에 또 실행하면 에러 발생 ==> 개발중이니까 overwrite 되도록 코드를 짜자.
		if (hdfs.exists(outDir)) {
			hdfs.delete(outDir, true);
		}
		hdfs.close();
		
		FileInputFormat.setInputPaths(job, inDir);
		FileOutputFormat.setOutputPath(job, outDir);
		
		// true ==> job이 끝날때까지 대기한다.
		job.waitForCompletion(true);
		
		
	}

}
