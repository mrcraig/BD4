import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;


public class RTCompare implements Comparator<RevTime>{

	@Override
	public int compare(RevTime rt1, RevTime rt2) {
		SimpleDateFormat dateComp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		Date t1 = null;
		Date t2 = null;
		try {
			t1 = dateComp.parse(rt1.getTs());
			t2 = dateComp.parse(rt2.getTs());
		} catch (ParseException e) {
			System.out.println("Unable to parse date");
		}
		
		if(t1.before(t2))
			return 1;
		else if(t1.after(t2))
			return -1;
		else
			return 0;
		
	}

}
