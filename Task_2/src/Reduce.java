import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
      String startDate;
      String endDate;
	
	 @Override
     public void setup (Context context){
             startDate = (context.getConfiguration().getStrings("Dates")[0]);
             endDate = (context.getConfiguration().getStrings("Dates")[1]);
     }
	 
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                 
        	String[] v;
			
			LongWritable artID = key;
			String ts;

			ArrayList<Long> revs = new ArrayList<Long>();
			int revno = 0;
			for (Text val:values){
				v = val.toString().split(";");
				ts = v[0];
				if (ts.compareTo(startDate)>=0 && ts.compareTo(endDate)<=0){
					revs.add(Long.parseLong(v[1]));;
					revno++;
				}
			}
			
			Long[] revisions;
			
			String results = "";
			if (!revs.isEmpty()){
				revisions = new Long[revs.size()];
				revisions = revs.toArray(revisions);
				Arrays.sort(revisions);
				results = "";
				for(Long rev : revs){
						results += String.valueOf(rev)+" ";
				}
				context.write(artID, new Text( revno + " " + results + " "));
			}
        }      
      
}