import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reduce extends Reducer<LongWritable, RevTime, LongWritable, RevTime> {
	
	public void reduce(LongWritable key, Iterable<RevTime> values, Context context) throws IOException, InterruptedException {
		//Storage to provide ordering
		ArrayList<RevTime> sortArr = new ArrayList<RevTime>(100);

		for(RevTime rt:values){
			sortArr.add(new RevTime(rt.getRev(),rt.getTs()));
		}
		
		Collections.sort(sortArr,new RTCompare());
		
		context.write(key, sortArr.get(0));
	}
}