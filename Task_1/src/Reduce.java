import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reduce extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
		public void run(Context context) throws IOException, InterruptedException{
			
			//Declare storage for intermediate sorting step
			ArrayList<KeyValue> store = new ArrayList<KeyValue>(500);
			
			while(context.nextKey()){
				
				addSortByKey(context.getCurrentKey(),context.getValues());
				reduce(context.getCurrentKey(), context.getValues(), context);
			}
		}
		
		public void addSortByKey(LongWritable artid, Iterable<LongWritable> revid){
		//	KV = new KeyValue(Long.parseLong(artid.toString()),revid.)
		}
	}