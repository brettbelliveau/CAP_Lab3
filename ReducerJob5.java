import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerJob5 extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
	
	public Text title = new Text();
	
    public void reduce(DoubleWritable score, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	for (Text link : values)
    	context.write(link, score);
    }
}
