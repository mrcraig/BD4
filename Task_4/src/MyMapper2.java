import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Scanner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class MyMapper2 extends Mapper<LongWritable, Text, IntWritable, KeyValue>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		//Process value text
		Scanner recordScan = new Scanner(value.toString());
		long keyV = recordScan.nextLong();
		int valV = recordScan.nextInt();
		recordScan.close();
		KeyValue newKV = new KeyValue(keyV,valV);
		
		context.write(new IntWritable(1), newKV);
	}
}
