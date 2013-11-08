import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.io.FileReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		
		BufferedReader f = null;
		f = new BufferedReader(new FileReader("file:///home/cloudera/Documents/index_file"));
		
		long artID = 0;
		long revID= 0;
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
		
		String[] stuff;
		String l;
		
		if ((l = f.readLine())!=null) {
			stuff = l.split("\t");
			time = stuff[0]; // Timestamp
			artID = Long.parseLong(stuff[1]); // article ID
			revID = Long.parseLong(stuff[2]); // revision id
			

				// Parse timestamp from data
				try {
					ts = dateFormat.parse(time);
				} catch (ParseException e) {
					System.out.println("Error - Date formatted incorrectly ("+ time + ")");
					e.printStackTrace();
				}

				// Emit if in range
				if (ts.after(startDate) && ts.before(endDate))
					context.write(new LongWritable(artID), new LongWritable(revID));
		}
		f.close();
	}
}
