import java.util.Comparator;


public class KeyValue {
	private long key;
	private int value;
	
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
}
