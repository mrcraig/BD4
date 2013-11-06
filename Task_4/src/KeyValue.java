import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import org.apache.hadoop.io.Writable;


public class KeyValue implements Writable {
	private long key;
	private int value;
	
	public KeyValue(){
		this.key=0;
		this.value=0;
	}
	
	public KeyValue(long key, int value){
		this.key = key;
		this.value = value;
	}
	
	public long getKey(){
		return this.key;
	}
	
	public int getValue(){
		return this.value;
	}
	
	public int compare(KeyValue first, KeyValue second) {
		if(first.getValue()<second.getValue())
			return -1;
		else if(first.getValue()>second.getValue())
			return 1;
		else 
			return 0;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.key = in.readLong();
		this.value = in.readInt();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(key);
		out.writeInt(value);
		
	}
}
