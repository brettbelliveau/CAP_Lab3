import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MRReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        
    	/* WordCount Code for reference
    	int sum = 0;
        for(IntWritable v : values)
            sum += v.get();
        result.set(sum);

        context.write(key, result);
        */
    }
}