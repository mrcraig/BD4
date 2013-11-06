import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Scanner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class Reduce2 extends Reducer<IntWritable, KeyValue, LongWritable, IntWritable> {
	public void reduce(IntWritable key, Iterable<KeyValue> value, Context context) throws IOException, InterruptedException {
		//Add collection storage
		ArrayList<KeyValue> sortArray = new ArrayList<KeyValue>(150);
		Comparator comp = new KVCompare();
		
		
		while(value.iterator().hasNext()){
			sortArray.add(value.iterator().next());
		}
		
		Collections.sort(sortArray, comp);
		int noElems = context.getConfiguration().getInt("recordstodisplay", 0);
		for(int i=0;i<noElems;i++){
			context.write(new LongWritable(sortArray.get(i).getKey()), new IntWritable(sortArray.get(i).getValue()));
		}
		
	}
}
