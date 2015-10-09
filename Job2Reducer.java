import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job2Reducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
    	for(Text t : values) {
        	if (t == "$$$") {
        		context.write(key,"");
        	}
        	else {
        		context.write(key, t);
        	}
        }
    }
}