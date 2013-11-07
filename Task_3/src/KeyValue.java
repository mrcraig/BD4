import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;


public class KeyValue {
	private LongWritable key;
	private Iterable<LongWritable> value;
	
	public KeyValue(LongWritable key, Iterable<LongWritable> value){
		this.key = key;
		this.value = value;
	}
	
	public LongWritable getKey(){
		return this.key;
	}
	
	public Iterable<LongWritable> getValue(){
		return this.value;
	}
	
	/*
	 * Return -1 if object calling comparison is less than
	 * 		   1 if object calling comparison is more than
	 * 		   0 if objects are the same
	 */
	public int compareTo(KeyValue kv2){
		if(this.key.get()<kv2.getKey().get())
			return -1;
		else if(this.key.get()>kv2.getKey().get())
			return 1;
		else
			return 0;
	}
}
