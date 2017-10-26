package com.world.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// (keyin, valuein) ----변환----> (keyout, valueout)   : hadoop에 있는 type만 쓸 수 있다.
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		System.out.println("########");
		System.out.println("setup...");
		System.out.println("########");
	}
	
	static int cnt;
	//Mapper가 실행되면 key = 파일의 시작위치, value = 한 line 이 들어온다.
	//map은 입력 라인당 한번씩 호출된다.
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		System.out.println("cnt = " + cnt++ + " : " + key.get() + " : " + value);
		
		//콤마를 기준으로 split => String 배열로 return
		String[] values = value.toString().split(",");

		//write된 결과는 Reducer로 넘어간다.
		for (String v : values) {
			context.write(new Text(v), new IntWritable(1));
			System.out.println(v + "\t1");
		}
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("##########");
		System.out.println("cleanup...");
		System.out.println("##########");
	}
}
