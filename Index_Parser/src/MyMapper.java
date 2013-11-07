import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		
		long artID = 0;
		long revID = 0;
		String artTitle;
		Date ts = null;
		String time = "";
		String record = value.toString();
		
		
		//Output articleId and revid
		Scanner lineScan = new Scanner(record);
		if(lineScan.hasNext()){
			
			String linetitle = lineScan.next();	//REVISION TITLE
			if(linetitle.equals("REVISION")){
				artID = lineScan.nextLong();	//article ID
				revID = lineScan.nextLong();	//revision ID
				lineScan.next();				//article title
				time = lineScan.next();			//Timestamp
			
				context.write(new Text(time), new Text(artID + "\t" + revID));
			}
		}
	}
}
