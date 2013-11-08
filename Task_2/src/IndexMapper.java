import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class IndexMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
        
                public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

            		BufferedReader f = null;
            		f = new BufferedReader(new FileReader("blahblahblah arse"));
            		
            		LongWritable artID = new LongWritable();
            		LongWritable revID = new LongWritable();
            		String time = "";
            		
            		Date ts = null;
            		Date startDate = null;
            		Date endDate = null;

            		// Parse Date
            		// 2013-11-03T00:45Z
            		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

            		try {
            			startDate = dateFormat.parse(context.getConfiguration().getStrings("Dates")[0]);
            			endDate = dateFormat.parse(context.getConfiguration().getStrings("Dates")[1]);
            		} catch (ParseException e) {
            			String var = context.getConfiguration().getStrings("Dates")[1];
            			System.out.println("Error - Date formatted incorrectly (" + var+ ")");
            		}
            		
            		// Output articleId and revid
            		
            		String[] stuff;
            		String l;
            		
            		if ((l = f.readLine())!=null) {
            			stuff = l.split(" ");
            			time = stuff[0]; // Timestamp
            			artID.set(Long.parseLong(stuff[1])); // article ID
            			revID.set(Long.parseLong(stuff[2])); // revision id
            			
                        //Parse timestamp from data
                        try {
                                ts = dateFormat.parse(time);
                        } catch (ParseException e) {
                                System.out.println("Error - Date formatted incorrectly (" + time + ")");
                                e.printStackTrace();
                        }
                        
                        Text val = new Text(time + ";" + revID.toString());
                        
                        context.write(artID,val);
                }
                
                
        }
                
}