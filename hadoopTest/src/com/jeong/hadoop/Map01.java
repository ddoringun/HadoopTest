package com.jeong.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map01 extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	// 오타를 방지하기 위해서라도 Override를 사용해야함
	// 오타가 있으면 내부에 메소드를 하나 더 만든게 되어버리기 때문에 main에서 접근할 수 없음
	@Override
	public void map(LongWritable key, Text value, Context ctx) 
			throws IOException, InterruptedException {
		AirParser ap = new AirParser(value);
		
		if(ap.getCarrierDelay_sol() > AirParser.NONDELAY_SOL) { //NONDELAY_SOL = 0
			ctx.write(new Text(ap.getUniqueCarrier_sol() + "_" + ap.getTailNum_sol()),
					new IntWritable(ap.getCarrierDelay_sol()));
		}else {
			ctx.write(new Text(ap.getUniqueCarrier_sol() + "_" + ap.getTailNum_sol()),
					new IntWritable(AirParser.NONDELAY_SOL));
		}
	}
}
