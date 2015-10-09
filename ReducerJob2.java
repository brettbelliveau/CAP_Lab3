import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerJob2 extends Reducer<Text, Text, Text, Text> {
	
    private Text link = new Text();
	
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String str = "";
    	for(Text t : values) {
        	if (t.toString().contains("$$$")) {
        		str += "";
            	str += '\t';
        	}
        	else if (!str.contains(t.toString())){
        		str += t.toString();
            	str += '\t';
        	}
        }
    	link.set(str);
		context.write(key, link);
    }
}