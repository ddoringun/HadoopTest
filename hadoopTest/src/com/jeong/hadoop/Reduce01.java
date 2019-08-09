package com.jeong.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce01 extends Reducer<Text, IntWritable, Text, DoubleWritable>{
	public void reduce(Text key, Iterable<IntWritable> values, Context ctx)
	throws IOException, InterruptedException{
		int delay = 0; // 지연시간
		int allCnt = 0; // 총 지연시간
		int delayCnt = 0; // 지연 운항횟수
		double delayRate = 0; // 평균 지연시간
		
		while(values.iterator().hasNext()) {
			delay = values.iterator().next().get();  // values에 값이 있으면 정수화 시켜서 delay에 값 저장
			allCnt++;
			if(delay > 0) { 
				delayCnt++;
			}
		}
		// 지연율 계산
		delayRate = ((double)delayCnt / allCnt);   // 지연운항횟수에서 총 지연시간을 나눠줌
		
		ctx.write(key, new DoubleWritable(delayRate));
	}
}
