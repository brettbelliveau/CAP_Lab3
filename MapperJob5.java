import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperJob5 extends Mapper<LongWritable, Text, DoubleWritable, Text>{
	
	private Text key = new Text();
	private DoubleWritable score = new DoubleWritable();
	private double N = 0.0;

    public void map(LongWritable scorelw, Text values, Context context) throws IOException, InterruptedException {
    	Configuration conf = context.getConfiguration();
    	String Nstr = conf.get("NumberOfItems");
    	String split[] = Nstr.split("=");
    	split[1] = split[1].trim();
    	N = Long.parseLong(split[1]);
    	
    	String line = values.toString();
    	
        StringTokenizer itr = new StringTokenizer(line);
        
    	key.set(itr.nextToken());
    	double num = Double.parseDouble(itr.nextToken());
    	score.set(num);
    	
    	if (num > (5/N))
    		context.write(score, key);
	}
}
