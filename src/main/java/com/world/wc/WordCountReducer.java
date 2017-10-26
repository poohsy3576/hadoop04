package com.world.wc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Mapper (keyout, valueout) = Reducer (keyin, valuein)
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	//Context는 Reducer 안에 있는 내부 클래스
	//각 그룹별로 한번씩 reduce 호출
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		List <Integer> list = new ArrayList<>();
		
		// key = xxx, value = [1,1,1,1] 등으로 넘어간다. ==> value를 sum하면 wordcount가 되는 것이다.
		int sum = 0;
		for (IntWritable v : values) {
			list.add(v.get());
			sum += v.get();
		}
		
		context.write(key, new IntWritable(sum));
		
		System.out.println("key = " + key + ", value = " + list);
	}
}
