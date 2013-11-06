import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class RevTime implements Writable{
	private long rev;
	private String ts;
	
	public RevTime(){
		this.rev = 0;
		this.ts = "";
	}
	
	public RevTime(long rev, String ts){
		this.rev = rev;
		this.ts = ts;
	}
	
	public long getRev(){
		return this.rev;
	}
	
	public String getTs(){
		return this.ts;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.rev = in.readLong();
		this.ts = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.rev);
		out.writeUTF(ts);
	}
	
	public String toString(){
		return rev + " " + ts;
	}
}
