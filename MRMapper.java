import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MRMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
    
	private final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
    	/* WordCount code for reference
    	String line = value.toString();
        StringTokenizer itr = new StringTokenizer(line.toLowerCase(), " .,:;!?{}[]()|_");
        while(itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }*/
    }
}