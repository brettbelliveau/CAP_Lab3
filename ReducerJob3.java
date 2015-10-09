import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerJob3 extends Reducer<Text, Text, Text, Text> {
    private Text text = new Text();
    private Text empty = new Text();
    
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	int N = 0;
    	
    	for(Text text : values) {
        	++N;
        }
    	String str = "N=" + N; 
    	text.set(str);
    	empty.set("");
		context.write(text, empty);
    }
}