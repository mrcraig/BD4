import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Task_4_2B extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		// Set conf variables pointing to the BD4 cluster 
		conf.set("fs.defaultFS", "hdfs://bigdata-06.dcs.gla.ac.uk:8020");
		conf.set("mapred.job.tracker", "bigdata-06.dcs.gla.ac.uk:8021");

		// Hadoop needs to upload the jar with your code to HDFS, so instruct it where to find it
		// Supply a full URI as the second argument; e.g., "file:///C:/Users/foo/some/dir/myjar.jar"
		// if you are on Windows, "file:///home/foo/some/dir/myjar.jar" if you are on *nix, etc.
		conf.set("mapred.jar", "file:///users/level4/1002386c/Documents/BD4/myjar_2.jar");

		// Delete the output directory to allow the job to run
		// !!! CAUTION !!! Make sure you are not deleting something you need !!! CAUTION !!!
		FileSystem fs = FileSystem.get(conf);

		// Now do the standard boilerplate stuff, only using the above
		// prepopulated configuration object...
		Job job = new Job(conf);
		job.setJobName("Task 4_2B");
		job.setJarByClass(Task_4_2B.class);
		job.setMapperClass(IndexMapper.class);
		job.setReducerClass(Reduce.class);
		//job.setInputFormatClass(NLinesInputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//Store date in config
		job.getConfiguration().setStrings("Dates", args[2], args[3]);
		
		//Set number of records to display in config
		int noRecords = 0;
		try{
			noRecords = Integer.parseInt(args[4]);
		} catch (NumberFormatException e){
			System.out.println("Number of records to display incorrectly formatted.");
		}
		
		job.getConfiguration().setInt("recordstodisplay", noRecords);

		// ... and you're done! If you now execute this, with the Hadoop
		// libraries in your classpath, it will spawn a new job on the BD4
		// cluster. You can monitor its progress and errors by browsing to:
		// http://bigdata-06.dcs.gla.ac.uk:50030
		job.submit();
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Task_4B(), args));
	}
}