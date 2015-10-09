import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperJob2 extends Mapper<LongWritable, Text, Text, Text>{
	
    private Text k = new Text();
    private Text v = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
    	String line = value.toString();
        StringTokenizer itr = new StringTokenizer(line);
        while (itr.hasMoreTokens()){
        	k.set(itr.nextToken());
	        if (itr.hasMoreTokens()){
	        	v.set(itr.nextToken());
	        	context.write(v,k);
	        }
        }
    }
}