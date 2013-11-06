import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reduce extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
	
	public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			
			int numRecs = 0;
			while(values.iterator().hasNext()){
				numRecs++;
			}
			
			context.write(key,new IntWritable(numRecs));
	}
}