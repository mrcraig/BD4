import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{


		LongWritable artID = new LongWritable();
		LongWritable revID = new LongWritable();
		Date ts = null;
		String time = "";
		String record = value.toString();
		Date startDate = null;
		Date endDate = null;

		//Parse Date
		//2013-11-03T00:45Z
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

		try {
			startDate = dateFormat.parse(context.getConfiguration().getStrings("Dates")[0]);
			endDate = dateFormat.parse(context.getConfiguration().getStrings("Dates")[1]);
		} catch (ParseException e) {
			String var = context.getConfiguration().getStrings("Dates")[1];
			System.out.println("Error - Date formatted incorrectly (" + var + ")");
		}


		//Output articleId and revid
		Scanner lineScan = new Scanner(record);
		if(lineScan.hasNext()){

			String linetitle = lineScan.next();	//REVISION TITLE
			if(linetitle.equals("REVISION")){
				artID.set(lineScan.nextLong());	//article ID
				revID.set(lineScan.nextLong());	//revision ID
				lineScan.next();				//article title
				time = lineScan.next();		//Timestamp

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

			lineScan.close();

		}
	}
}