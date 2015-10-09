import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperJob3 extends Mapper<LongWritable, Text, Text, Text>{

	private Text text = new Text();
	
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	text.set("doritos");
        context.write(text,value);
    }
}
