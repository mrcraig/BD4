import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MyMapper extends Mapper<LongWritable, Text, LongWritable, RevTime>{
	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		
		long artid = 0;
		long revid = 0;
		Date ts = null;
		Date inDate = null;
		String time = "";
		String record = value.toString();
		
		//Parse Date
		//2013-11-03T00:45Z
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		
		try {
			inDate = dateFormat.parse(context.getConfiguration().getStrings("Date")[0]);
		} catch (ParseException e) {
			String var = context.getConfiguration().getStrings("Dates")[0];
			System.out.println("Error - Date formatted incorrectly (" + var + ")");
		}
		
		//Output articleId and revid
		if(record.contains("REVISION")){
			Scanner lineScan = new Scanner(record);
			lineScan.next();				//REVISION TITLE
			artid = lineScan.nextLong();	//article ID
			revid = lineScan.nextLong();	//revision ID
			lineScan.next();				//article title
			time = lineScan.next();			//Timestamp
		
			try {
				ts = dateFormat.parse(time);
			} catch (ParseException e) {
				System.out.println("Error - Date formatted incorrectly (" + time + ")");
			}
			
			//Emit if in range
			if(inDate.after(ts))
				context.write(new LongWritable(artid),new RevTime(revid,time));
			
			lineScan.close();
		}
		
		
	}
		
}
