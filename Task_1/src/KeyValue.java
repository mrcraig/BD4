
public class KeyValue {
	private long key;
	private long value;
	
	public KeyValue(long key, long value){
		this.key = key;
		this.value = value;
	}
	
	public long getKey(){
		return this.key;
	}
	
	public long getValue(){
		return this.value;
	}
	
	/*
	 * Return -1 if object calling comparison is less than
	 * 		   1 if object calling comparison is more than
	 * 		   0 if objects are the same
	 */
	public int compareTo(KeyValue kv2){
		if(this.key<kv2.getKey())
			return -1;
		else if(this.key>kv2.getKey())
			return 1;
		else
			return 0;
	}
}
