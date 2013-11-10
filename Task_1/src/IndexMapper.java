import java.io.BufferedReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;
import java.io.FileReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		if(!context.getConfiguration().getBoolean("finished", false)){
			LongWritable artID = new LongWritable();
			LongWritable revID = new LongWritable();		
			Date ts = null;
			String time = "";
	
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
			
			Scanner indexScan = new Scanner(value.toString());
			time = indexScan.next(); // Timestamp
			artID.set(indexScan.nextLong()); // article ID
			revID.set(indexScan.nextLong()); // revision id
			
	
				// Parse timestamp from data
				try {
					ts = dateFormat.parse(time);
				} catch (ParseException e) {
					System.out.println("Error - Date formatted incorrectly ("+ time + ")");
					e.printStackTrace();
				}
			
				if(ts.after(endDate))
					context.getConfiguration().setBoolean("finished", true);
				
	
				// Emit if in range
				if (ts.after(startDate) && !context.getConfiguration().getBoolean("finished", false))
					context.write(artID, revID);
	
		}
	}
}
