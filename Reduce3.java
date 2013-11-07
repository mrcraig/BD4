
public class Reduce3 {
	import java.io.IOException;
	import java.text.SimpleDateFormat;
	import java.util.ArrayList;
	import java.util.Collections;
	import java.util.Arrays;

	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.LongWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Reducer;
	import org.apache.hadoop.mapreduce.Reducer.Context;

	public class Reduce extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
	      String startDate;
	      String endDate;
	      int minrevs
		
		 @Override
	     public void setup (Context context){
	             Configuration conf = context.getConfiguration();
	             startDate = conf.get("startDate");
	             endDate = conf.get("endDate");
	             minrevs = conf.get("minrevs");
	     }
		 
	        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
	                 
	        	String[] v;
				v = values.split(";");
				Long artID = key;
				String ts;

				ArrayList<Long> revs = new ArrayList<Long>();

				for (Text val:values){
					Long revID = Long.parseLong(v[0]);
					ts = v[1];
					if (ts.compareTo(startDate)<=0 && ts.compareTo(endDate)>=0){
					revs.add(Long.parseLong(ts));;
					}
				}
				
	            if (revs.size() > minrevs){
	            	
	            	context.write(artID, new Text(revs.size()));
	            }
	            
	            
				//stuff craig did
	                        //Storage
	/*                        ArrayList<Long> sortArray = new ArrayList<Long>();
	                        
	                        while(values.iterator().hasNext()){
	                                sortArray.add(values.iterator().next().get());
	                        }
	                        
	                        Collections.sort(sortArray);
	                                
	                        for(long val:sortArray)
	                                context.write(key, new LongWritable(val));
	                        
	                        sortArray.clear();
	     */   }
	}
}
