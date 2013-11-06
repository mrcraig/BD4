import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reduce extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
	
	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			
			//Storage
			ArrayList<Long> sortArray = new ArrayList<Long>();
			
			while(values.iterator().hasNext()){
				sortArray.add(values.iterator().next().get());
			}
			
			Collections.sort(sortArray);
				
			for(long val:sortArray)
				context.write(key, new LongWritable(val));
			
			sortArray.clear();
	}
}