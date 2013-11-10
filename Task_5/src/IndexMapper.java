import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class IndexMapper extends Mapper<LongWritable, Text, LongWritable, RevTime>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		LongWritable artID = new LongWritable();
		long revID = 0;
		String time = "";

		Date ts = null;
		Date inDate = null;
		// Parse Date
		// 2013-11-03T00:45Z
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

		try {
			inDate = dateFormat.parse(context.getConfiguration().getStrings("Date")[0]);
		} catch (ParseException e) {
			String var = context.getConfiguration().getStrings("Dates")[0];
			System.out.println("Error - Date formatted incorrectly (" + var + ")");
		}

		// Output articleId and revid

		String[] stuff;
		String l;

			Scanner indexScan = new Scanner(value.toString());
			time = indexScan.next(); // Timestamp
			artID.set(indexScan.nextLong()); // article ID
			revID = indexScan.nextLong(); // revision id

			//Parse timestamp from data
			try {
				ts = dateFormat.parse(time);
			} catch (ParseException e) {
				System.out.println("Error - Date formatted incorrectly (" + time + ")");
				e.printStackTrace();
			}	

				//Emit if in range
				if(inDate.after(ts))
					context.write(artID,new RevTime(revID,time));//WHY
		}

	}
