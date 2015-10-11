import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperJob4 extends Mapper<LongWritable, Text, Text, Text>{
	
	private Text title_and_size = new Text();
	private Text title = new Text();
	private Text no_links = new Text();
	private Text link = new Text();

    public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
    
    	ArrayList<String> list = new ArrayList<>();
    	String line = values.toString();
    	StringTokenizer str = new StringTokenizer(line);
    	
    	while(str.hasMoreTokens())
    		list.add(str.nextToken());
    	
        int items = (list.size() - 1);
        
        title.set(list.get(0));
        no_links.set(title.toString() + '\t' + "###");
        context.write(title,no_links);
        
        String t_and_s = list.get(0) + '\t' + items;
        title_and_size.set(t_and_s);
        
        String linkstr = "";
        for (int index = 1; index < list.size(); index++){
        	linkstr = list.get(index);
        	link.set(linkstr);
            context.write(link, title_and_size);
        }
    }
}
