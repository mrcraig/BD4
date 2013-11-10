import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

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
		
				int noofrevs = revs.size();
				
	            if (noofrevs> minrevs){
	            	context.write(artID, new Text(Integer.toString(noofrevs)));
	            }
	           
	}
}
