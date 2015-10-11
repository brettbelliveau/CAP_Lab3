import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerJob5 extends Reducer<DoubleWritable, Text, Text, Text> {
	
	public Text title = new Text();
	public Text score = new Text();
	
    public void reduce(DoubleWritable score, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	String line = values.toString();
    	line = line.trim();
    	this.score.set("" + score.toString());
    	title.set(line);
    	context.write(title, this.score);
    }
}
