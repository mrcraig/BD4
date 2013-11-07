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

	public class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
	      String startDate;
	      String endDate;
	      int minrevs;
		
		 @Override
	     public void setup (Context context){
			 	startDate = (context.getConfiguration().getStrings("Dates")[0]);
			 	endDate = (context.getConfiguration().getStrings("Dates")[1]);
	            minrevs = context.getConfiguration().getInt("minrevs", 1);
		 }
		 
	        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	                 
	            
	        	String[] v;
				
				LongWritable artID = key;
				String ts;

				ArrayList<Long> revs = new ArrayList<Long>();
				//int revno = 0;
				for (Text val:values){
					v = val.toString().split(";");
					ts = v[0];
					if (ts.compareTo(startDate)>=0 && ts.compareTo(endDate)<=0){
						revs.add(Long.parseLong(v[1]));;
						//revno++;
					}
				}
				
				Long[] revisions;
				
				String results = "";
			/*	if (!revs.isEmpty()){
					revisions = new Long[revs.size()];
					revisions = revs.toArray(revisions);
					Arrays.sort(revisions);
					results = "";
					for(Long rev : revs){
							results += String.valueOf(rev)+" ";
					}
					//context.write(artID, new Text( revno + " " + results + " "));
				}*/
				
				int noofrevs = revs.size();
				
	            if (noofrevs> minrevs){
	            	context.write(artID, new Text(Integer.toString(noofrevs)));
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
	     */   
	}
}
