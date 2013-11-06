import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Scanner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class Reduce2 extends Reducer<IntWritable, KeyValue, LongWritable, IntWritable> {
	public void reduce(IntWritable key, Iterable<KeyValue> value, Context context) throws IOException, InterruptedException {
		//Add collection storage
		LinkedList<KeyValue> sortArray = new LinkedList<KeyValue>();
		Comparator comp = new KVCompare();
		
//		
//		for(KeyValue kv:value){
//			context.write(new LongWritable(kv.getKey()), new IntWritable(kv.getValue()));
//		}
//		
//		for(KeyValue kvpair:value){
//			sortArray.addFirst(kvpair);
//		}
		
		//Collections.sort(sortArray,new KVCompare());
		
		
		//Doesn't like adding keyvalue without creating new keyvalue apparently.. grr. so many hours.
		int noElems = context.getConfiguration().getInt("recordstodisplay", 0);
		for(KeyValue k:value){
			sortArray.add(new KeyValue(k.getKey(),k.getValue()));
		}
		
		Collections.sort(sortArray,new KVCompare());
		
		for(int i=0;i<noElems;i++){
			context.write(new LongWritable(sortArray.get(i).getKey()), new IntWritable(sortArray.get(i).getValue()));
		}
		
	}
}
