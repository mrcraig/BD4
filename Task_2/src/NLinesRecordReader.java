import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;


public class NLinesRecordReader extends RecordReader<LongWritable, Text> {
	private final int TOPROCESS = 13;
	private LineReader ln;
	private long start=0;
	private long end=0;
	private long pos=0;
	private LongWritable key;
	private Text value;
	private long maxlength;
	

	@Override
	public void close() throws IOException {
		if(ln!=null)
			ln.close();
		
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if(start==end)
			return 0.0f;
		else
			return Math.min(1.0f, (pos-start)/(end-start));
	}

	@Override
	public void initialize(InputSplit gsplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSplit split = (FileSplit) gsplit;
		final Path file = split.getPath();
		Configuration conf = context.getConfiguration();
        this.maxlength = conf.getInt("mapred.linerecordreader.maxlength",Integer.MAX_VALUE);
        FileSystem fs = file.getFileSystem(conf);
        start = split.getStart();
        end= start + split.getLength();
        boolean skipFirstLine = false;
        FSDataInputStream filein = fs.open(split.getPath());
 
        if (start != 0){
            skipFirstLine = true;
            --start;
            filein.seek(start);
        }
        ln = new LineReader(filein,conf);
        if(skipFirstLine){
            start += ln.readLine(new Text(),0,(int)Math.min((long)Integer.MAX_VALUE, end - start));
        }
        this.pos = start;
    }
		

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null) {
            key = new LongWritable();
        }
        key.set(pos);
        if (value == null) {
            value = new Text();
        }
        value.clear();
        final Text endline = new Text("\n");
        int newSize = 0;
        for(int i=0;i<TOPROCESS;i++){
            Text v = new Text();
            while (pos < end) {
                newSize = ln.readLine(v, (int)maxlength,(int)Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),maxlength));
                value.append(v.getBytes(),0, v.getLength());
                value.append(endline.getBytes(),0, endline.getLength());
                if (newSize == 0) {
                    break;
                }
                pos += newSize;
                if (newSize < maxlength) {
                    break;
                }
            }
        }
        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
    }

}
