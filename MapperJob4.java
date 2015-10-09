import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperJob4 extends Mapper<LongWritable, Text, Text, Text>{
	
	private Text title = new Text();
	private Text links = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer itr = new StringTokenizer(line);
        int index = 0;
        String linkstr = "";
        while(itr.hasMoreTokens()) {
        	if (index == 0)
        		title.set(itr.nextToken());
        	else
        		linkstr += itr.nextToken() + '\t';
        	++index;
        }
        links.set(linkstr);
        context.write(title, links);	
    }
}
