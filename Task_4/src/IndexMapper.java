import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class IndexMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

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

			Scanner indexScanner = new Scanner(value.toString());
			time = indexScanner.next();			 // Timestamp
			artID.set(indexScanner.nextLong()); // article ID
			revID.set(indexScanner.nextLong()); // revision id

			//Parse timestamp from data
			try {
				ts = dateFormat.parse(time);
			} catch (ParseException e) {
				System.out.println("Error - Date formatted incorrectly (" + time + ")");
				e.printStackTrace();
			}

			//Emit if in range
			//TODO sort
			if(ts.after(startDate) && ts.before(endDate))
				context.write(artID,new IntWritable(1));

	}

}
